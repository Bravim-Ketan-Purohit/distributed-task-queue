import { useQuery } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'
import { fetchTask, type AttemptInfo } from '../api'
import { Clock, Shield, AlertCircle, CheckCircle } from 'lucide-react'

const outcomeColors: Record<string, string> = {
  succeeded: 'text-green-400',
  failed: 'text-red-400',
  lease_lost: 'text-yellow-400',
  timeout: 'text-orange-400',
}

export default function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>()

  const { data: task, isLoading, error } = useQuery({
    queryKey: ['task', taskId],
    queryFn: () => fetchTask(taskId!),
    enabled: !!taskId,
  })

  if (!taskId) {
    return <div className="text-gray-400">Enter a task ID in the URL: /tasks/:id</div>
  }

  if (isLoading) return <div className="text-gray-400">Loading...</div>
  if (error) return <div className="text-red-400">Task not found</div>
  if (!task) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <h2 className="text-2xl font-semibold text-white">Task Detail</h2>
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
          task.state === 'succeeded' ? 'bg-green-900/30 text-green-400' :
          task.state === 'failed' || task.state === 'dead' ? 'bg-red-900/30 text-red-400' :
          task.state === 'leased' ? 'bg-yellow-900/30 text-yellow-400' :
          'bg-blue-900/30 text-blue-400'
        }`}>
          {task.state}
        </span>
      </div>

      {/* Task metadata */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <dl className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
          <div>
            <dt className="text-gray-500">ID</dt>
            <dd className="text-gray-200 font-mono text-xs mt-1 break-all">{task.id}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Task Name</dt>
            <dd className="text-gray-200 mt-1">{task.task_name}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Queue</dt>
            <dd className="text-gray-200 mt-1">{task.queue}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Priority</dt>
            <dd className="text-gray-200 mt-1">{task.priority}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Attempt</dt>
            <dd className="text-gray-200 mt-1">{task.attempt} / {task.max_attempts}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Dedup Key</dt>
            <dd className="text-gray-200 font-mono text-xs mt-1">{task.dedup_key || '—'}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Created</dt>
            <dd className="text-gray-200 mt-1 text-xs">{new Date(task.created_at).toLocaleString()}</dd>
          </div>
          <div>
            <dt className="text-gray-500">Updated</dt>
            <dd className="text-gray-200 mt-1 text-xs">{new Date(task.updated_at).toLocaleString()}</dd>
          </div>
        </dl>
      </div>

      {/* Payload */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <h3 className="text-sm font-medium text-gray-400 mb-2">Payload</h3>
        <pre className="text-xs text-gray-300 bg-gray-950 rounded-lg p-3 overflow-auto max-h-48">
          {JSON.stringify(task.payload, null, 2)}
        </pre>
      </div>

      {/* Attempt timeline */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <h3 className="text-sm font-medium text-gray-400 mb-4">Attempt Timeline</h3>
        {task.attempts.length === 0 ? (
          <p className="text-gray-500 text-sm">No attempts recorded yet.</p>
        ) : (
          <div className="space-y-3">
            {task.attempts.map((a: AttemptInfo) => (
              <div key={a.attempt_no} className="flex items-start gap-3 p-3 bg-gray-950 rounded-lg">
                <div className="mt-0.5">
                  {a.outcome === 'succeeded' ? (
                    <CheckCircle className="w-4 h-4 text-green-400" />
                  ) : a.outcome === 'lease_lost' ? (
                    <Shield className="w-4 h-4 text-yellow-400" />
                  ) : (
                    <AlertCircle className="w-4 h-4 text-red-400" />
                  )}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 text-sm">
                    <span className="font-medium text-gray-200">Attempt #{a.attempt_no}</span>
                    <span className={`text-xs ${outcomeColors[a.outcome || ''] || 'text-gray-400'}`}>
                      {a.outcome || 'running'}
                    </span>
                  </div>
                  <div className="mt-1 text-xs text-gray-400 space-x-4">
                    <span>Worker: <span className="font-mono text-gray-300">{a.worker_id}</span></span>
                    <span>Fence: <span className="font-mono text-gray-300">{a.fence}</span></span>
                    <span className="inline-flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      {new Date(a.started_at).toLocaleTimeString()}
                      {a.finished_at && ` → ${new Date(a.finished_at).toLocaleTimeString()}`}
                    </span>
                  </div>
                  {a.error_repr && (
                    <pre className="mt-2 text-xs text-red-300 bg-red-950/30 p-2 rounded overflow-auto max-h-24">
                      {a.error_repr}
                    </pre>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
