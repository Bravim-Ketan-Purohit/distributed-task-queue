import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchWorkers, chaosKillWorker, chaosPauseQueue, chaosInjectFailure, type WorkerInfo } from '../api'
import { Zap, Skull, Pause, Bug } from 'lucide-react'

export default function ChaosPanel() {
  const { data: workers } = useQuery({ queryKey: ['workers'], queryFn: fetchWorkers })

  const [killTarget, setKillTarget] = useState('')
  const [pauseQueue, setPauseQueue] = useState('default')
  const [pauseDuration, setPauseDuration] = useState(30)
  const [failQueue, setFailQueue] = useState('default')
  const [failTask, setFailTask] = useState('chaos_test')
  const [log, setLog] = useState<string[]>([])

  const addLog = (msg: string) => {
    setLog((prev) => [`[${new Date().toLocaleTimeString()}] ${msg}`, ...prev].slice(0, 20))
  }

  const handleKill = async () => {
    if (!killTarget) return
    try {
      await chaosKillWorker(killTarget)
      addLog(`Killed worker: ${killTarget}`)
    } catch (e) {
      addLog(`Kill failed: ${e}`)
    }
  }

  const handlePause = async () => {
    try {
      await chaosPauseQueue(pauseQueue, pauseDuration)
      addLog(`Paused queue "${pauseQueue}" for ${pauseDuration}s`)
    } catch (e) {
      addLog(`Pause failed: ${e}`)
    }
  }

  const handleInjectFailure = async () => {
    try {
      await chaosInjectFailure(failQueue, failTask)
      addLog(`Injected failing task "${failTask}" into "${failQueue}"`)
    } catch (e) {
      addLog(`Inject failed: ${e}`)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <Zap className="w-6 h-6 text-yellow-400" />
        <h2 className="text-2xl font-semibold text-white">Chaos Panel</h2>
        <span className="px-2 py-0.5 bg-yellow-900/30 text-yellow-400 text-xs rounded-full">
          DTQ_ENABLE_CHAOS=1 required
        </span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {/* Kill Worker */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Skull className="w-4 h-4 text-red-400" />
            <h3 className="text-sm font-medium text-gray-200">Kill Worker</h3>
          </div>
          <label htmlFor="kill-worker-select" className="sr-only">Select worker to kill</label>
          <select
            id="kill-worker-select"
            value={killTarget}
            onChange={(e) => setKillTarget(e.target.value)}
            className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 mb-3"
          >
            <option value="">Select worker...</option>
            {workers?.map((w: WorkerInfo) => (
              <option key={w.worker_id} value={w.worker_id}>
                {w.worker_id} {w.alive ? '(alive)' : '(dead)'}
              </option>
            ))}
          </select>
          <button
            onClick={handleKill}
            disabled={!killTarget}
            className="w-full px-3 py-2 bg-red-700 hover:bg-red-600 disabled:opacity-50
              rounded-lg text-sm text-white transition-colors"
          >
            Kill
          </button>
        </div>

        {/* Pause Queue */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Pause className="w-4 h-4 text-orange-400" />
            <h3 className="text-sm font-medium text-gray-200">Pause Queue</h3>
          </div>
          <label htmlFor="pause-queue-input" className="sr-only">Queue name</label>
          <input
            id="pause-queue-input"
            type="text"
            value={pauseQueue}
            onChange={(e) => setPauseQueue(e.target.value)}
            placeholder="Queue name"
            className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 mb-2"
          />
          <label htmlFor="pause-duration-input" className="sr-only">Duration in seconds</label>
          <input
            id="pause-duration-input"
            type="number"
            value={pauseDuration}
            onChange={(e) => setPauseDuration(Number(e.target.value))}
            className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 mb-3"
          />
          <button
            onClick={handlePause}
            className="w-full px-3 py-2 bg-orange-700 hover:bg-orange-600
              rounded-lg text-sm text-white transition-colors"
          >
            Pause
          </button>
        </div>

        {/* Inject Failure */}
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-4">
            <Bug className="w-4 h-4 text-purple-400" />
            <h3 className="text-sm font-medium text-gray-200">Inject Failure</h3>
          </div>
          <label htmlFor="inject-queue-input" className="sr-only">Queue name</label>
          <input
            id="inject-queue-input"
            type="text"
            value={failQueue}
            onChange={(e) => setFailQueue(e.target.value)}
            placeholder="Queue"
            className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 mb-2"
          />
          <label htmlFor="inject-task-input" className="sr-only">Task name</label>
          <input
            id="inject-task-input"
            type="text"
            value={failTask}
            onChange={(e) => setFailTask(e.target.value)}
            placeholder="Task name"
            className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-sm text-gray-200 mb-3"
          />
          <button
            onClick={handleInjectFailure}
            className="w-full px-3 py-2 bg-purple-700 hover:bg-purple-600
              rounded-lg text-sm text-white transition-colors"
          >
            Inject
          </button>
        </div>
      </div>

      {/* Activity log */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
        <h3 className="text-sm font-medium text-gray-400 mb-3">Chaos Log</h3>
        {log.length === 0 ? (
          <p className="text-sm text-gray-500">No chaos actions taken yet.</p>
        ) : (
          <ul className="space-y-1 font-mono text-xs" role="log" aria-label="Chaos action log">
            {log.map((entry, i) => (
              <li key={i} className="text-gray-300">{entry}</li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
