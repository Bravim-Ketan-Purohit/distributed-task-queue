import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { fetchDLQ, requeueTasks, type DLQTask } from '../api'
import { Skull, RotateCcw } from 'lucide-react'

export default function DLQPage() {
  const [queue, setQueue] = useState('default')
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const queryClient = useQueryClient()

  const { data: tasks, isLoading } = useQuery({
    queryKey: ['dlq', queue],
    queryFn: () => fetchDLQ(queue),
  })

  const requeueMutation = useMutation({
    mutationFn: () => requeueTasks(queue, Array.from(selected)),
    onSuccess: () => {
      setSelected(new Set())
      queryClient.invalidateQueries({ queryKey: ['dlq', queue] })
    },
  })

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const selectAll = () => {
    if (!tasks) return
    if (selected.size === tasks.length) {
      setSelected(new Set())
    } else {
      setSelected(new Set(tasks.map((t) => t.task_id)))
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Skull className="w-6 h-6 text-red-400" />
          <h2 className="text-2xl font-semibold text-white">Dead Letter Queue</h2>
        </div>
        <div className="flex items-center gap-3">
          <label htmlFor="dlq-queue-select" className="sr-only">Select queue</label>
          <input
            id="dlq-queue-select"
            type="text"
            value={queue}
            onChange={(e) => setQueue(e.target.value)}
            placeholder="Queue name"
            className="px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200
              focus:ring-1 focus:ring-blue-500 focus:border-blue-500 outline-none"
          />
          <button
            onClick={() => requeueMutation.mutate()}
            disabled={selected.size === 0 || requeueMutation.isPending}
            className="flex items-center gap-2 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:opacity-50
              disabled:cursor-not-allowed rounded-lg text-sm text-white transition-colors"
            aria-label={`Requeue ${selected.size} selected tasks`}
          >
            <RotateCcw className="w-4 h-4" />
            Requeue ({selected.size})
          </button>
        </div>
      </div>

      <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
        {isLoading ? (
          <div className="p-8 text-center text-gray-500">Loading...</div>
        ) : !tasks || tasks.length === 0 ? (
          <div className="p-8 text-center text-gray-500">No dead-lettered tasks in "{queue}".</div>
        ) : (
          <table className="w-full text-sm" role="table">
            <thead>
              <tr className="border-b border-gray-800">
                <th className="px-4 py-3 text-left">
                  <input
                    type="checkbox"
                    checked={selected.size === tasks.length}
                    onChange={selectAll}
                    className="rounded border-gray-600"
                    aria-label="Select all"
                  />
                </th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Task ID</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Task Name</th>
                <th className="px-4 py-3 text-center text-gray-400 font-medium">Attempts</th>
                <th className="px-4 py-3 text-left text-gray-400 font-medium">Reason</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800">
              {tasks.map((t: DLQTask) => (
                <tr key={t.task_id} className="hover:bg-gray-800/50">
                  <td className="px-4 py-3">
                    <input
                      type="checkbox"
                      checked={selected.has(t.task_id)}
                      onChange={() => toggleSelect(t.task_id)}
                      className="rounded border-gray-600"
                      aria-label={`Select task ${t.task_id.slice(0, 8)}`}
                    />
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-300">
                    {t.task_id.slice(0, 8)}...
                  </td>
                  <td className="px-4 py-3 text-gray-200">{t.task_name}</td>
                  <td className="px-4 py-3 text-center text-gray-400">{t.attempt}</td>
                  <td className="px-4 py-3 text-red-300 text-xs truncate max-w-xs">{t.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
