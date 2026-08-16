import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { fetchQueues, subscribeEvents, type QueueInfo, type TaskEvent } from '../api'
import { Activity, ArrowUpDown, Clock, AlertTriangle } from 'lucide-react'

export default function Overview() {
  const { data: queues, isLoading } = useQuery({
    queryKey: ['queues'],
    queryFn: fetchQueues,
  })

  const [events, setEvents] = useState<TaskEvent[]>([])

  useEffect(() => {
    const es = subscribeEvents((event) => {
      setEvents((prev) => [event, ...prev].slice(0, 50))
    })
    return () => es.close()
  }, [])

  if (isLoading) {
    return <div className="text-gray-400">Loading...</div>
  }

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-semibold text-white">Queue Overview</h2>

      {/* Queue cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {queues?.map((q: QueueInfo) => (
          <div key={q.queue} className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-medium text-gray-300">{q.queue}</h3>
              <Activity className="w-4 h-4 text-blue-400" />
            </div>
            <dl className="grid grid-cols-2 gap-3">
              <div>
                <dt className="text-xs text-gray-500">Depth</dt>
                <dd className="text-xl font-semibold text-white">{q.depth}</dd>
              </div>
              <div>
                <dt className="text-xs text-gray-500">In-Flight</dt>
                <dd className="text-xl font-semibold text-yellow-400">{q.in_flight}</dd>
              </div>
              <div>
                <dt className="text-xs text-gray-500">Throughput</dt>
                <dd className="text-sm text-gray-300 flex items-center gap-1">
                  <ArrowUpDown className="w-3 h-3" />
                  {q.throughput.toFixed(1)}/s
                </dd>
              </div>
              <div>
                <dt className="text-xs text-gray-500">Oldest</dt>
                <dd className="text-sm text-gray-300 flex items-center gap-1">
                  <Clock className="w-3 h-3" />
                  {q.oldest_pending_age_s.toFixed(1)}s
                </dd>
              </div>
            </dl>
            {q.error_rate > 0 && (
              <div className="mt-3 flex items-center gap-1 text-red-400 text-xs">
                <AlertTriangle className="w-3 h-3" />
                {(q.error_rate * 100).toFixed(1)}% error rate
              </div>
            )}
          </div>
        ))}
        {(!queues || queues.length === 0) && (
          <div className="col-span-full text-center text-gray-500 py-8">
            No queues active. Enqueue a task to get started.
          </div>
        )}
      </div>

      {/* Live event feed */}
      <div>
        <h3 className="text-lg font-medium text-white mb-3">Live Events</h3>
        <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
          {events.length === 0 ? (
            <div className="p-4 text-gray-500 text-sm text-center">
              Waiting for events...
            </div>
          ) : (
            <ul className="divide-y divide-gray-800 max-h-72 overflow-y-auto" role="log" aria-label="Live task events">
              {events.map((e, i) => (
                <li key={i} className="px-4 py-2 flex items-center gap-3 text-sm">
                  <span className={`inline-block w-2 h-2 rounded-full ${
                    e.state === 'succeeded' ? 'bg-green-400' :
                    e.state === 'failed' || e.state === 'dead' ? 'bg-red-400' :
                    e.state === 'leased' ? 'bg-yellow-400' :
                    'bg-blue-400'
                  }`} />
                  <span className="text-gray-400 font-mono text-xs w-20 shrink-0">
                    {e.event_type}
                  </span>
                  <span className="text-gray-300 font-mono text-xs truncate">
                    {e.task_id.slice(0, 8)}...
                  </span>
                  <span className="text-gray-500 ml-auto text-xs">
                    {e.queue}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}
