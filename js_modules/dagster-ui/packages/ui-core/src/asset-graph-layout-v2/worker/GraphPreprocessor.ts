import {getLayoutGraph} from './GraphLayout';
import {updateProcessingProgress} from './workerUtil';
import {GraphData, groupIdForNode} from '../../asset-graph/Utils';
import {AssetNode, GroupNode, ModelGraph, ModelNode, NodeType} from '../common/ModelGraph';
import {ProcessingLabel} from '../common/WorkerEvents';
import {DEFAULT_GROUP_NODE_CHILDREN_COUNT_THRESHOLD} from '../common/conts';
import {Edge} from '../common/types';
import {isAssetNode, isGroupNode} from '../common/utils';

/**
 * A class that processes given `GraphData` into a `ModelGraph`.
 */
export class GraphProcessor {
  constructor(
    private readonly paneId: string,
    private readonly graph: GraphData,
    private readonly groupNodeChildrenCountThreshold = DEFAULT_GROUP_NODE_CHILDREN_COUNT_THRESHOLD,
    private readonly testMode = false,
    private readonly flattenLayers = false,
  ) {}

  process(): ModelGraph {
    const modelGraph = this.createEmptyModelGraph();

    this.processNodes(modelGraph);
    this.processEdgeRelationships(modelGraph);
    updateProcessingProgress(this.paneId, ProcessingLabel.PROCESSING_NODES_AND_EDGES);

    this.generateLayoutGraphConnections(modelGraph);
    updateProcessingProgress(this.paneId, ProcessingLabel.PROCESSING_LAYOUT_DATA);

    this.splitLargeGroupNodes(modelGraph);
    updateProcessingProgress(this.paneId, ProcessingLabel.SPLITTING_LARGE_LAYERS);

    this.populateDescendantsAndCounts(modelGraph);

    return modelGraph;
  }

  /**
   * Scans nodes in `Graph` and creates the corresponding `AssetNode` and
   * `GroupNode` in the `ModelGraph` (see model_graph.ts for more details).
   */
  processNodes(modelGraph: ModelGraph) {
    const seenGroups = new Set<string>();
    for (const graphNode of Object.values(this.graph.nodes)) {
      const group = groupIdForNode(graphNode);
      const assetNode: AssetNode = {
        nodeType: NodeType.ASSET_NODE,
        id: graphNode.id,
        level: 1,
        namespace: `${group}/${graphNode.id}`,
      };
      modelGraph.nodes.push(assetNode);
      modelGraph.nodesById[assetNode.id] = assetNode;

      if (!assetNode.hideInLayout && !this.flattenLayers) {
        if (!seenGroups.has(group)) {
          const groupNode: GroupNode = {
            nodeType: NodeType.GROUP_NODE,
            id: group,
            level: 0,
            expanded: false,
            namespace: group,
          };
          seenGroups.add(group);
          modelGraph.nodes.push(groupNode);
          modelGraph.nodesById[groupNode.id] = groupNode;
        }
      }
    }
  }

  /**
   * Sets edges in the given model graph based on the edges in the input graph.
   */
  processEdgeRelationships(modelGraph: ModelGraph) {
    for (const graphNode of Object.values(this.graph.nodes)) {
      const node = modelGraph.nodesById[graphNode.id] as AssetNode;
      if (!node) {
        continue;
      }

      const incomingEdges = Object.keys(this.graph.upstream[graphNode.id] || {}) || [];

      // From the graph node's incoming edges, populate the incoming and
      // outgoing edges for the corresponding node in the model graph.
      for (const sourceNodeId of incomingEdges) {
        const incomingEdge: Edge = {
          sourceNodeId,
          targetNodeId: graphNode.id,
        };
        const sourceNode = modelGraph.nodesById[sourceNodeId] as AssetNode;
        if (!sourceNode) {
          continue;
        }

        // Incoming edges.
        if (node.incomingEdges == null) {
          node.incomingEdges = [];
        }
        if (node.incomingEdges.find((edge) => edge.sourceNodeId === sourceNodeId) == null) {
          node.incomingEdges.push({...incomingEdge});
        }

        // Outgoing edges.
        if (sourceNode.outgoingEdges == null) {
          sourceNode.outgoingEdges = [];
        }
        if (sourceNode.outgoingEdges.find((edge) => edge.targetNodeId === node.id) == null) {
          sourceNode.outgoingEdges.push({
            targetNodeId: node.id,
            sourceNodeId: incomingEdge.sourceNodeId,
          });
        }
      }
    }
  }

  /**
   * Generates layout graph connections for the given model graph.
   */
  generateLayoutGraphConnections(modelGraph: ModelGraph) {
    modelGraph.layoutGraphEdges = {};

    // Find all op nodes that don't have incoming edges.
    const assetNodesWithoutIncomingEdges: AssetNode[] = [];
    for (const node of modelGraph.nodes) {
      if (!isAssetNode(node) || node.hideInLayout) {
        continue;
      }
      const filteredIncomingEdges = (node.incomingEdges || []).filter(
        (edge) => !(modelGraph.nodesById[edge.sourceNodeId] as AssetNode).hideInLayout,
      );
      if (filteredIncomingEdges.length === 0) {
        assetNodesWithoutIncomingEdges.push(node);
      }
    }

    // Do a BFS from assetNodesWithoutIncomingEdges.
    const queue: AssetNode[] = [...assetNodesWithoutIncomingEdges];
    const seenNodeIds = new Set<string>();
    while (queue.length > 0) {
      const curNode = queue.shift();
      if (curNode == null || curNode.hideInLayout) {
        continue;
      }
      if (seenNodeIds.has(curNode.id)) {
        continue;
      }
      seenNodeIds.add(curNode.id);

      const outgoingEdges = curNode.outgoingEdges || [];
      for (const edge of outgoingEdges) {
        const targetNode = modelGraph.nodesById[edge.targetNodeId] as AssetNode;
        queue.push(targetNode);
      }
    }
  }

  /**
   * Finds group nodes with a large number of children, and splits them into
   * different groups
   */
  splitLargeGroupNodes(modelGraph: ModelGraph) {
    // From root, do a BFS search on all group nodes.
    const queue: Array<GroupNode | undefined> = [undefined];
    let hasLargeGroupNodes = false;
    while (queue.length > 0) {
      const curGroupNode = queue.shift();
      let children: ModelNode[] =
        curGroupNode == null
          ? modelGraph.rootNodes
          : (curGroupNode.childrenIds || []).map((id) => modelGraph.nodesById[id]!);

      // Split the group node if its child count is over the threshold.
      if (children.length > this.groupNodeChildrenCountThreshold) {
        hasLargeGroupNodes = true;
        const layoutGraph = getLayoutGraph(
          curGroupNode?.id || '',
          children,
          modelGraph,
          this.testMode,
          // Use fake node size.
          true,
        );

        // Find root nodes of the layout graph.
        const rootNodes: ModelNode[] = [];
        for (const nodeId of Object.keys(layoutGraph.nodes)) {
          if (layoutGraph.incomingEdges[nodeId] == null) {
            rootNodes.push(modelGraph.nodesById[nodeId]!);
          }
        }

        // Do a DFS from the layout graph root nodes. Create a new group
        // whenever the node counts reaches the threshold.
        const groups: ModelNode[][] = [];
        let curGroup: ModelNode[] = [];
        const visitedNodeIds = new Set<string>();
        const visit = (curNodeId: string) => {
          if (visitedNodeIds.has(curNodeId)) {
            return;
          }
          visitedNodeIds.add(curNodeId);
          const node = modelGraph.nodesById[curNodeId]!;
          curGroup.push(node);
          if (curGroup.length === this.groupNodeChildrenCountThreshold) {
            groups.push(curGroup);
            curGroup = [];
          }
          for (const childId of layoutGraph.outgoingEdges[node.id] || []) {
            visit(childId);
          }
        };
        for (const rootNode of rootNodes) {
          visit(rootNode.id);
        }
        if (curGroup.length < this.groupNodeChildrenCountThreshold && curGroup.length > 0) {
          groups.push(curGroup);
        }

        // Create a new group node for each group.
        const newGroupNodes: GroupNode[] = [];
        for (let groupIndex = 0; groupIndex < groups.length; groupIndex++) {
          const nodes = groups[groupIndex]!;
          const newGroupNodeNamespace =
            curGroupNode == null ? '' : `${curGroupNode.namespace}/${curGroupNode.id}`;
          const baseId = `section_${groupIndex + 1}_of_${groups.length}`;
          const newGroupNodeId = `${baseId}___group___`;
          const newGroupNode: GroupNode = {
            nodeType: NodeType.GROUP_NODE,
            id: newGroupNodeId,
            namespace: newGroupNodeNamespace,
            level: 0,
            parentId: curGroupNode?.id,
            childrenIds: nodes.map((node) => node.id),
            expanded: false,
            sectionContainer: true,
          };
          newGroupNodes.push(newGroupNode);

          // Add the new group node to the model graph.
          modelGraph.nodes.push(newGroupNode);
          modelGraph.nodesById[newGroupNode.id] = newGroupNode;
          if (modelGraph.artificialGroupNodeIds == null) {
            modelGraph.artificialGroupNodeIds = [];
          }
          modelGraph.artificialGroupNodeIds.push(newGroupNode.id);

          // Update the ns parent for all nodes in the new group.
          for (const node of nodes) {
            node.parentId = newGroupNode.id;
          }

          // Update the namespace of all nodes and their desendents in the new
          // group.
          const newNamespacePart = newGroupNodeId.replace('___group___', '');
          const updateNamespace = (node: ModelNode) => {
            const oldNamespace = node.namespace;
            if (oldNamespace === '') {
              node.namespace = newNamespacePart;
            } else {
              if (curGroupNode == null) {
                node.namespace = `${newNamespacePart}/${node.namespace}`;
              } else {
                node.namespace = (node.parentId || '').replace('___group___', '');
              }
            }
            node.level = node.namespace.split('/').filter((c) => c !== '').length;
            if (isGroupNode(node)) {
              // Update group node id since its namespace has been changed.
              const oldNodeId = node.id;
              delete modelGraph.nodesById[node.id];
              node.id = `${node.namespace}/${node.id}___group___`;
              modelGraph.nodesById[node.id] = node;

              // Update its parent's children to use the new id.
              if (node.parentId) {
                const nsParent = modelGraph.nodesById[node.parentId] as GroupNode;
                const index = (nsParent.childrenIds || []).indexOf(oldNodeId);
                if (index >= 0) {
                  (nsParent.childrenIds || [])[index] = node.id;
                }
              }

              for (const childId of node.childrenIds || []) {
                const childNode = modelGraph.nodesById[childId];
                if (childNode != null) {
                  // Update its children's nsParent id.
                  childNode.parentId = node.id;
                  // BFS.
                  updateNamespace(childNode);
                }
              }
            }
          };
          for (const node of nodes) {
            updateNamespace(node);
          }

          if (curGroupNode == null) {
            // Remove the nodes in the current new group if they are in the root
            // node list.
            for (const node of nodes) {
              const index = modelGraph.rootNodes.indexOf(node);
              if (index >= 0) {
                modelGraph.rootNodes.splice(index, 1);
              }
            }

            // Add the new group node to root node list if its namespace is
            // empty.
            if (newGroupNode.namespace === '') {
              modelGraph.rootNodes.push(newGroupNode);
            }
          }

          children = newGroupNodes;
        }

        // Update curGrassetNode's childrenIds.
        if (curGroupNode != null) {
          curGroupNode.childrenIds = newGroupNodes.map((node) => node.id);
        }
      }

      for (const child of children) {
        if (isGroupNode(child)) {
          queue.push(child);
        }
      }
    }

    if (hasLargeGroupNodes) {
      this.generateLayoutGraphConnections(modelGraph);
    }
  }

  populateDescendantsAndCounts(modelGraph: ModelGraph) {
    // For each group node, gather all its descendant nodes.
    let minAssetNodeCount = Number.MAX_VALUE;
    let maxAssetNodeCount = Number.NEGATIVE_INFINITY;
    for (const node of modelGraph.nodes) {
      if (isGroupNode(node)) {
        const descendants: ModelNode[] = [];
        this.gatherDescendants(modelGraph, node, descendants);
        node.descendantsNodeIds = descendants.map((node) => node.id);
        node.descendantsAssetNodeIds = descendants
          .filter((node) => node.nodeType === NodeType.ASSET_NODE)
          .map((node) => node.id);
        const assetNodeCount = (node.descendantsAssetNodeIds || []).length;
        minAssetNodeCount = Math.min(assetNodeCount, minAssetNodeCount);
        maxAssetNodeCount = Math.max(assetNodeCount, maxAssetNodeCount);
      }
    }
    modelGraph.minDescendantAssetNodeCount = minAssetNodeCount;
    modelGraph.maxDescendantAssetNodeCount = maxAssetNodeCount;
  }

  createEmptyModelGraph(): ModelGraph {
    return {
      id: '<ROOT>',
      nodes: [],
      nodesById: {},
      rootNodes: [],
      edgesByGroupNodeIds: {},
      layoutGraphEdges: {},
      minDescendantAssetNodeCount: -1,
      maxDescendantAssetNodeCount: -1,
    };
  }

  private gatherDescendants(modelGraph: ModelGraph, curRoot: GroupNode, descendants: ModelNode[]) {
    for (const childId of curRoot.childrenIds || []) {
      const child = modelGraph.nodesById[childId];
      if (isGroupNode(child) || (isAssetNode(child) && !child.hideInLayout)) {
        descendants.push(child);
      }
      if (isGroupNode(child)) {
        this.gatherDescendants(modelGraph, child, descendants);
      }
    }
  }
}
