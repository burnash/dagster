import {GraphExpander} from './GraphExpander';
import {GraphLayout} from './GraphLayout';
import {GraphProcessor} from './GraphPreprocessor';
import {updateProcessingProgress} from './workerUtil';
import {GraphData} from '../../asset-graph/Utils';
import {GroupNode, ModelGraph} from '../common/ModelGraph';
import {
  ExpandOrCollapseGroupNodeResponse,
  ProcessGraphResponse,
  ProcessingLabel,
  RelayoutGraphResponse,
  WorkerEvent,
  WorkerEventType,
} from '../common/WorkerEvents';
import {getDeepestExpandedGroupNodeIds, isGroupNode} from '../common/utils';

// <rendererId + ModelGraphId> -> ModelGraph
let MODEL_GRAPHS_CACHE: Record<string, ModelGraph> = {};

self.addEventListener('message', (event) => {
  const workerEvent = event.data as WorkerEvent;
  switch (workerEvent.eventType) {
    // Handle processing input graph.
    case WorkerEventType.PROCESS_GRAPH_REQ: {
      const modelGraph = handleProcessGraph(
        workerEvent.graphId,
        workerEvent.graph,
        workerEvent.initialLayout,
      );
      cacheModelGraph(modelGraph, workerEvent.graphId);
      const resp: ProcessGraphResponse = {
        eventType: WorkerEventType.PROCESS_GRAPH_RESP,
        modelGraph,
        graphId: workerEvent.graphId,
      };
      postMessage(resp);
      break;
    }
    case WorkerEventType.EXPAND_OR_COLLAPSE_GROUP_NODE_REQ: {
      const modelGraph = getCachedModelGraph(workerEvent.modelGraphId, workerEvent.rendererId);
      let deepestExpandedGroupNodeIds: string[] = [];
      if (workerEvent.expand) {
        deepestExpandedGroupNodeIds = handleExpandGroupNode(
          modelGraph,
          workerEvent.groupNodeId,
          workerEvent.all === true,
        );
      } else {
        deepestExpandedGroupNodeIds = handleCollapseGroupNode(
          modelGraph,
          workerEvent.groupNodeId,
          workerEvent.all === true,
        );
      }
      cacheModelGraph(modelGraph, workerEvent.rendererId);
      const resp: ExpandOrCollapseGroupNodeResponse = {
        eventType: WorkerEventType.EXPAND_OR_COLLAPSE_GROUP_NODE_RESP,
        modelGraph,
        expanded: workerEvent.expand,
        groupNodeId: workerEvent.groupNodeId,
        rendererId: workerEvent.rendererId,
        deepestExpandedGroupNodeIds,
      };
      postMessage(resp);
      break;
    }
    case WorkerEventType.RELAYOUT_GRAPH_REQ: {
      const modelGraph = getCachedModelGraph(workerEvent.modelGraphId, workerEvent.rendererId);
      handleReLayoutGraph(
        modelGraph,
        workerEvent.targetDeepestGroupNodeIdsToExpand,
        workerEvent.clearAllExpandStates,
      );
      cacheModelGraph(modelGraph, workerEvent.rendererId);
      const resp: RelayoutGraphResponse = {
        eventType: WorkerEventType.RELAYOUT_GRAPH_RESP,
        modelGraph,
        selectedNodeId: workerEvent.selectedNodeId,
        rendererId: workerEvent.rendererId,
        forRestoringUiState: workerEvent.forRestoringUiState,
        rectToZoomFit: workerEvent.rectToZoomFit,
        forRestoringSnapshotAfterTogglingFlattenLayers:
          workerEvent.forRestoringSnapshotAfterTogglingFlattenLayers,
        targetDeepestGroupNodeIdsToExpand: workerEvent.targetDeepestGroupNodeIdsToExpand,
      };
      postMessage(resp);
      break;
    }
    case WorkerEventType.CLEANUP: {
      MODEL_GRAPHS_CACHE = {};
      break;
    }
    default:
      break;
  }
});

function handleProcessGraph(
  graphId: string,
  graph: GraphData,
  initialLayout?: boolean,
): ModelGraph {
  let error: string | undefined = undefined;

  // Processes the given input graph `Graph` into a `ModelGraph`.
  const processor = new GraphProcessor(graphId, graph);
  const modelGraph = processor.process();

  // Check nodes with empty ids.
  if (modelGraph.nodesById[''] != null) {
    error =
      'Some nodes have empty strings as ids which will cause layout failures. See console for details.';
    console.warn('Nodes with empty ids', modelGraph.nodesById['']);
  }

  // Do the initial layout.
  if (!error && initialLayout) {
    const layout = new GraphLayout(modelGraph);
    try {
      layout.layout();
    } catch (e) {
      error = `Failed to layout graph: ${e}`;
    }
  }
  updateProcessingProgress(graphId, ProcessingLabel.LAYING_OUT_ROOT_LAYER, error);
  return modelGraph;
}

function handleExpandGroupNode(
  modelGraph: ModelGraph,
  groupNodeId: string | undefined,
  all: boolean,
): string[] {
  const expander = new GraphExpander(modelGraph);

  // Expane group node.
  if (groupNodeId != null) {
    let deepestExpandedGroupNodeId: string[] | undefined = undefined;
    const groupNode = modelGraph.nodesById[groupNodeId];
    if (groupNode && isGroupNode(groupNode)) {
      groupNode.expanded = true;
      // Recursively expand child group node if there is only one child.
      let curGroupNode = groupNode;
      while (true) {
        const childrenIds = curGroupNode.childrenIds || [];
        if (childrenIds.length === 1) {
          const child = modelGraph.nodesById[childrenIds[0]!];
          if (child && isGroupNode(child)) {
            child.expanded = true;
            curGroupNode = child;
          } else {
            break;
          }
        } else {
          break;
        }
      }
      // Get the deepest expanded group nodes from the curGroupNode and we will
      // be doing relayout from there.
      const ids: string[] = [];
      getDeepestExpandedGroupNodeIds(curGroupNode, modelGraph, ids);
      deepestExpandedGroupNodeId = ids.length === 0 ? [curGroupNode.id] : ids;
      // Clear layout data for all nodes under curGroupNode.
      //
      // This is necessary because the node overlay might have been changed so
      // we need to re-calculate the node sizes.
      for (const nodeId of curGroupNode.descendantsNodeIds || []) {
        const node = modelGraph.nodesById[nodeId]!;
        node.width = undefined;
        node.height = undefined;
      }
    }
    if (all) {
      for (const childNodeId of (groupNode as GroupNode).descendantsNodeIds || []) {
        const node = modelGraph.nodesById[childNodeId];
        if (isGroupNode(node)) {
          node.expanded = true;
        }
      }
      deepestExpandedGroupNodeId = undefined;
    }
    expander.reLayoutGraph(deepestExpandedGroupNodeId);

    const ids: string[] = [];
    getDeepestExpandedGroupNodeIds(undefined, modelGraph, ids);
    return ids;
  }
  // Expand all group nodes in the graph.
  else {
    return expander.expandAllGroups();
  }
}

function handleCollapseGroupNode(
  modelGraph: ModelGraph,
  groupNodeId: string | undefined,
  all: boolean,
): string[] {
  const expander = new GraphExpander(modelGraph);

  if (groupNodeId != null) {
    if (all) {
      const groupNode = modelGraph.nodesById[groupNodeId] as GroupNode;
      for (const childNodeId of groupNode.descendantsNodeIds || []) {
        const node = modelGraph.nodesById[childNodeId];
        if (isGroupNode(node)) {
          node.expanded = false;
          node.width = undefined;
          node.height = undefined;
          delete modelGraph.edgesByGroupNodeIds[node.id];
        }
      }
    }
    return expander.collapseGroupNode(groupNodeId);
  } else {
    return expander.collapseAllGroup();
  }
}

function handleReLayoutGraph(
  modelGraph: ModelGraph,
  targetDeepestGroupNodeIdsToExpand?: string[],
  clearAllExpandStates?: boolean,
) {
  const expander = new GraphExpander(modelGraph);
  expander.reLayoutGraph(targetDeepestGroupNodeIdsToExpand, clearAllExpandStates);
}

function cacheModelGraph(modelGraph: ModelGraph, rendererId: string) {
  MODEL_GRAPHS_CACHE[getModelGraphKey(modelGraph.id, rendererId)] = modelGraph;
}

function getCachedModelGraph(modelGraphId: string, rendererId: string): ModelGraph {
  const cachedModelGraph = MODEL_GRAPHS_CACHE[getModelGraphKey(modelGraphId, rendererId)];
  if (cachedModelGraph == null) {
    throw new Error(
      `ModelGraph with id "${modelGraphId}" not found for rendererId "${rendererId}"`,
    );
  }
  return cachedModelGraph;
}

function getModelGraphKey(modelGraphId: string, rendererId: string): string {
  return `${modelGraphId}___${rendererId}`;
}
