import {
  ProcessingLabel,
  UpdateProcessingProgressRequest,
  WorkerEventType,
} from '../common/WorkerEvents';

/** Updates processing progress. */
export function updateProcessingProgress(graphId: string, label: ProcessingLabel, error?: string) {
  const req: UpdateProcessingProgressRequest = {
    eventType: WorkerEventType.UPDATE_PROCESSING_PROGRESS,
    graphId,
    label,
    error,
  };
  postMessage(req);
}
