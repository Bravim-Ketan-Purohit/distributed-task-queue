import { useQuery } from '@tanstack/react-query'
import { fetchWorkers, type WorkerInfo } from '../api'
import { Heart, Server } from 'lucide-react'

export default function Workers() {
  const { data: workers, isLoading } = useQuery({
    queryKey: ['workers'],
    queryFn: fetchWorkers,
  })

  if (isLoading) {
    return <div className="text-gray-400">Loading...</div>
  }

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-semibold text-white">Worker Fleet</h2>

      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        <table className="w-full text-sm" role="table">
          <thead>
            <tr className="border-b border-gray-800">
              <th className="text-left px-4 py-3 text-gray-400 font-medium">Worker</th>
              <th className="text-left px-4 py-3 text-gray-400 font-medium">Queues</th>
              <th className="text-center px-4 py-3 text-gray-400 font-medium">In-Flight</th>
              <th className="text-center px-4 py-3 text-gray-400 font-medium">Concurrency</th>
              <th className="text-center px-4 py-3 text-gray-400 font-medium">Heartbeat</th>
              <th className="text-center px-4 py-3 text-gray-400 font-medium">Version</th>
              <th className="text-center px-4 py-3 text-gray-400 font-medium">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-800">
            {workers?.map((w: WorkerInfo) => (
              <tr
                key={w.worker_id}
                className={`${w.alive ? '' : 'opacity-50'}`}
              >
                <td className="px-4 py-3">
                  <div className="flex items-center gap-2">
                    <Server className="w-4 h-4 text-gray-500" />
                    <span className="font-mono text-gray-200">{w.worker_id}</span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex gap-1 flex-wrap">
                    {w.queues.map((q) => (
                      <span key={q} className="px-2 py-0.5 bg-gray-800 rounded text-xs text-gray-300">
                        {q}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="px-4 py-3 text-center text-yellow-400 font-medium">
                  {w.in_flight}
                </td>
                <td className="px-4 py-3 text-center text-gray-300">
                  {w.concurrency}
                </td>
                <td className="px-4 py-3 text-center">
                  <div className="flex items-center justify-center gap-1">
                    <Heart className={`w-3 h-3 ${w.heartbeat_age_s < 15 ? 'text-green-400' : 'text-red-400'}`} />
                    <span className="text-gray-300">{w.heartbeat_age_s.toFixed(1)}s</span>
                  </div>
                </td>
                <td className="px-4 py-3 text-center text-gray-500 text-xs">
                  {w.version}
                </td>
                <td className="px-4 py-3 text-center">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    w.alive ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'
                  }`}>
                    {w.alive ? 'alive' : 'dead'}
                  </span>
                </td>
              </tr>
            ))}
            {(!workers || workers.length === 0) && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-gray-500">
                  No workers connected.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
