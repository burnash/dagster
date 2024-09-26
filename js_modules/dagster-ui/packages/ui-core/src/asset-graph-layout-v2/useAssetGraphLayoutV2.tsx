import {useLayoutEffect, useMemo, useRef} from 'react';

import {WorkerEvent, WorkerEventType} from './common/WorkerEvents';
import {GraphData} from '../asset-graph/Utils';
import {LayoutAssetGraphOptions} from '../asset-graph/layout';

class AssetGraphLayoutWorker {
  private static _instance: AssetGraphLayoutWorker;
  public static getInstance() {
    if (!this._instance) {
      this._instance = new AssetGraphLayoutWorker();
    }
    return this._instance;
  }

  private worker: Worker;
  private constructor() {
    this.worker = new Worker(new URL('../workers/dagre_layout.worker', import.meta.url));
    this.worker.addEventListener('message', (event) => {
      const data = event.data as WorkerEvent;
      switch (data.eventType) {
        case WorkerEventType.PROCESS_GRAPH_RESP:
        case WorkerEventType.EXPAND_OR_COLLAPSE_GROUP_NODE_RESP:
        case WorkerEventType.RELAYOUT_GRAPH_RESP:
        case WorkerEventType.UPDATE_PROCESSING_PROGRESS:
          return;
      }
    });
  }

  public processGraph() {}
  public expandOrCollapseNode() {}
  public relayoutGraph() {}
  public cleanup() {}
}

export function useAssetGraphLayout(
  graphData: GraphData,
  expandedGroups: string[],
  opts: LayoutAssetGraphOptions,
) {
  const worker = AssetGraphLayoutWorker.getInstance();
  const graphId = useMemo(() => computeGraphId(graphData), [graphData]);
  const previousGraphId = useRef('');

  useLayoutEffect(() => {}, [graphId]);

  useLayoutEffect(() => {
    if (previousGraphId.current !== graphId) {
      // If the graph ID changed then we're creating a new graph from scratch which will
      // include the expanded groups so no need to send a request to expand / collapse groups.
      return;
    }
    // Send a request to expand/collapse groups.
  }, [graphId, expandedGroups]);

  previousGraphId.current = graphId;
}

function computeGraphId(graphData: GraphData) {
  // Make the cache key deterministic by alphabetically sorting all of the keys since the order
  // of the keys is not guaranteed to be consistent even when the graph hasn't changed.
  function recreateObjectWithKeysSorted(obj: Record<string, Record<string, boolean>>) {
    const newObj: Record<string, Record<string, boolean>> = {};
    Object.keys(obj)
      .sort()
      .forEach((key) => {
        newObj[key] = Object.keys(obj[key]!)
          .sort()
          .reduce(
            (acc, k) => {
              acc[k] = obj[key]![k]!;
              return acc;
            },
            {} as Record<string, boolean>,
          );
      });
    return newObj;
  }

  return JSON.stringify({
    downstream: recreateObjectWithKeysSorted(graphData.downstream),
    upstream: recreateObjectWithKeysSorted(graphData.upstream),
    nodes: Object.keys(graphData.nodes)
      .sort()
      .map((key) => graphData.nodes[key]),
  });
}
