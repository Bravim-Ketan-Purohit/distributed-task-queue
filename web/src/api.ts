/** API client for the DTQ control plane. */

const BASE = ''

export interface QueueInfo {
  queue: string
  depth: number
  in_flight: number
  throughput: number
  oldest_pending_age_s: number
  error_rate: number
}

export interface WorkerInfo {
  worker_id: string
  queues: string[]
  in_flight: number
  concurrency: number
  heartbeat_age_s: number
  version: string
  alive: boolean
}

export interface AttemptInfo {
  attempt_no: number
  worker_id: string
  fence: number
  started_at: string
  finished_at: string | null
  outcome: string | null
  error_type: string | null
  error_repr: string | null
}

export interface TaskDetail {
  id: string
  queue: string
  task_name: string
  payload: Record<string, unknown>
  state: string
  priority: number
  attempt: number
  max_attempts: number
  dedup_key: string | null
  run_at: string | null
  workflow_id: string | null
  step_name: string | null
  created_at: string
  updated_at: string
  attempts: AttemptInfo[]
}

export interface WorkflowStep {
  name: string
  task_name: string
  state: string
  task_id: string | null
}

export interface WorkflowDetail {
  id: string
  name: string
  state: string
  created_at: string
  steps: WorkflowStep[]
}

export interface DLQTask {
  task_id: string
  queue: string
  task_name: string
  payload: Record<string, unknown>
  attempt: number
  reason: string
}

export interface TaskEvent {
  event_type: string
  task_id: string
  queue: string
  state: string
  timestamp: string
  metadata: Record<string, unknown>
}

// --- API functions ---

export async function fetchQueues(): Promise<QueueInfo[]> {
  const res = await fetch(`${BASE}/v1/queues`)
  if (!res.ok) throw new Error(`Failed to fetch queues: ${res.status}`)
  return res.json()
}

export async function fetchWorkers(): Promise<WorkerInfo[]> {
  const res = await fetch(`${BASE}/v1/workers`)
  if (!res.ok) throw new Error(`Failed to fetch workers: ${res.status}`)
  return res.json()
}

export async function fetchTask(taskId: string): Promise<TaskDetail> {
  const res = await fetch(`${BASE}/v1/tasks/${taskId}`)
  if (!res.ok) throw new Error(`Failed to fetch task: ${res.status}`)
  return res.json()
}

export async function fetchWorkflow(workflowId: string): Promise<WorkflowDetail> {
  const res = await fetch(`${BASE}/v1/workflows/${workflowId}`)
  if (!res.ok) throw new Error(`Failed to fetch workflow: ${res.status}`)
  return res.json()
}

export async function fetchDLQ(queue: string): Promise<DLQTask[]> {
  const res = await fetch(`${BASE}/v1/dlq/${queue}`)
  if (!res.ok) throw new Error(`Failed to fetch DLQ: ${res.status}`)
  return res.json()
}

export async function requeueTasks(queue: string, taskIds: string[]): Promise<void> {
  await fetch(`${BASE}/v1/dlq/${queue}/requeue`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ task_ids: taskIds }),
  })
}

export async function chaosKillWorker(workerId: string): Promise<void> {
  await fetch(`${BASE}/v1/chaos/kill-worker`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ worker_id: workerId }),
  })
}

export async function chaosPauseQueue(queue: string, durationS: number): Promise<void> {
  await fetch(`${BASE}/v1/chaos/pause-queue`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queue, duration_s: durationS }),
  })
}

export async function chaosInjectFailure(queue: string, taskName: string): Promise<void> {
  await fetch(`${BASE}/v1/chaos/inject-failure`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ queue, task_name: taskName, error_type: 'InjectedError' }),
  })
}

export function subscribeEvents(onEvent: (event: TaskEvent) => void): EventSource {
  const es = new EventSource(`${BASE}/v1/events`)
  es.onmessage = (e) => {
    try {
      const event = JSON.parse(e.data) as TaskEvent
      onEvent(event)
    } catch { /* ignore parse errors */ }
  }
  return es
}
