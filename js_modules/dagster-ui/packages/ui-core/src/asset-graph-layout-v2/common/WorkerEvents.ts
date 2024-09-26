import {ModelGraph} from './ModelGraph';
import {Rect} from './types';
import {GraphData} from '../../asset-graph/Utils';

/** The base of all worker events. */
export declare interface WorkerEventBase {
  eventType: WorkerEventType;
}

export enum ProcessingLabel {
  PROCESSING_NODES_AND_EDGES = 'Processing nodes and edges',
  PROCESSING_LAYOUT_DATA = 'Processing layout data',
  SPLITTING_LARGE_LAYERS = 'Splitting large layers (if any)',
  LAYING_OUT_ROOT_LAYER = 'Laying out root layer',
}

/** All processing labels. */
export const ALL_PROCESSING_LABELS = [
  ProcessingLabel.PROCESSING_NODES_AND_EDGES,
  ProcessingLabel.PROCESSING_LAYOUT_DATA,
  ProcessingLabel.SPLITTING_LARGE_LAYERS,
  ProcessingLabel.LAYING_OUT_ROOT_LAYER,
];

export declare interface UpdateProcessingProgressRequest extends WorkerEventBase {
  eventType: WorkerEventType.UPDATE_PROCESSING_PROGRESS;
  graphId: string;
  label: ProcessingLabel;
  error?: string;
}

/** The request for processing an input graph. */
export declare interface ProcessGraphRequest extends WorkerEventBase {
  eventType: WorkerEventType.PROCESS_GRAPH_REQ;
  graph: GraphData;
  graphId: string;
  initialLayout?: boolean;
}

/** The response for processing an input graph. */
export declare interface ProcessGraphResponse extends WorkerEventBase {
  eventType: WorkerEventType.PROCESS_GRAPH_RESP;
  modelGraph: ModelGraph;
  graphId: string;
}

/** The request for expanding/collapsing a group node. */
export declare interface ExpandOrCollapseGroupNodeRequest extends WorkerEventBase {
  eventType: WorkerEventType.EXPAND_OR_COLLAPSE_GROUP_NODE_REQ;
  modelGraphId: string;
  // undefined when expanding/collapsing from root.
  groupNodeId?: string;
  expand: boolean;
  rendererId: string;
  graphId: string;
  // Expand or collapse all groups under the selected group.
  all?: boolean;
  // Timestamp of when the request is sent.
  ts?: number;
}

/** The response for expanding/collapsing a group node. */
export declare interface ExpandOrCollapseGroupNodeResponse extends WorkerEventBase {
  eventType: WorkerEventType.EXPAND_OR_COLLAPSE_GROUP_NODE_RESP;
  modelGraph: ModelGraph;
  expanded: boolean;
  // undefined when expanding/collapsing from root.
  groupNodeId?: string;
  rendererId: string;
  // These are the deepest group nodes (in terms of level) that none of its
  // child group nodes is expanded.
  deepestExpandedGroupNodeIds: string[];
}

/**
 * The request for re-laying out the whole graph, keeping the current
 * collapse/expand states for all group nodes.
 */
export declare interface RelayoutGraphRequest extends WorkerEventBase {
  eventType: WorkerEventType.RELAYOUT_GRAPH_REQ;
  modelGraphId: string;
  targetDeepestGroupNodeIdsToExpand?: string[];
  selectedNodeId: string;
  rendererId: string;
  forRestoringUiState?: boolean;
  rectToZoomFit?: Rect;
  clearAllExpandStates?: boolean;
  forRestoringSnapshotAfterTogglingFlattenLayers?: boolean;
}

/** The response for re-laying out the whole graph. */
export declare interface RelayoutGraphResponse extends WorkerEventBase {
  eventType: WorkerEventType.RELAYOUT_GRAPH_RESP;
  modelGraph: ModelGraph;
  selectedNodeId: string;
  rendererId: string;
  forRestoringUiState?: boolean;
  rectToZoomFit?: Rect;
  forRestoringSnapshotAfterTogglingFlattenLayers?: boolean;
  targetDeepestGroupNodeIdsToExpand?: string[];
}

/** Various worker event types. */
export enum WorkerEventType {
  PROCESS_GRAPH_REQ,
  PROCESS_GRAPH_RESP,
  EXPAND_OR_COLLAPSE_GROUP_NODE_REQ,
  EXPAND_OR_COLLAPSE_GROUP_NODE_RESP,
  RELAYOUT_GRAPH_REQ,
  RELAYOUT_GRAPH_RESP,
  UPDATE_PROCESSING_PROGRESS,
  CLEANUP,
}

export declare interface CleanupRequest extends WorkerEventBase {
  eventType: WorkerEventType.CLEANUP;
}

export type WorkerEvent =
  | ProcessGraphRequest
  | ProcessGraphResponse
  | ExpandOrCollapseGroupNodeRequest
  | ExpandOrCollapseGroupNodeResponse
  | RelayoutGraphRequest
  | RelayoutGraphResponse
  | UpdateProcessingProgressRequest
  | CleanupRequest;
