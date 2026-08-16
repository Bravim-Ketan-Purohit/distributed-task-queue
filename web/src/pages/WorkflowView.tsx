import { useQuery } from '@tanstack/react-query'
import { useParams } from 'react-router-dom'
import { fetchWorkflow, type WorkflowStep } from '../api'
import { GitBranch, CheckCircle, XCircle, Clock, Loader } from 'lucide-react'

const stateIcon = (state: string) => {
  switch (state) {
    case 'succeeded': return <CheckCircle className="w-4 h-4 text-green-400" />
    case 'dead':
    case 'failed': return <XCircle className="w-4 h-4 text-red-400" />
    case 'leased': return <Loader className="w-4 h-4 text-yellow-400 animate-spin" />
    default: return <Clock className="w-4 h-4 text-gray-400" />
  }
}

const stateColor = (state: string) => {
  switch (state) {
    case 'succeeded': return 'border-green-600 bg-green-950/20'
    case 'dead':
    case 'failed': return 'border-red-600 bg-red-950/20'
    case 'leased': return 'border-yellow-600 bg-yellow-950/20'
    default: return 'border-gray-700 bg-gray-900'
  }
}

export default function WorkflowView() {
  const { workflowId } = useParams<{ workflowId: string }>()

  const { data: workflow, isLoading, error } = useQuery({
    queryKey: ['workflow', workflowId],
    queryFn: () => fetchWorkflow(workflowId!),
    enabled: !!workflowId,
  })

  if (!workflowId) {
    return (
      <div className="text-center py-12 text-gray-500">
        <GitBranch className="w-12 h-12 mx-auto mb-4 text-gray-700" />
        <p>Enter a workflow ID in the URL: /workflows/:id</p>
      </div>
    )
  }

  if (isLoading) return <div className="text-gray-400">Loading...</div>
  if (error) return <div className="text-red-400">Workflow not found</div>
  if (!workflow) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <GitBranch className="w-6 h-6 text-purple-400" />
        <h2 className="text-2xl font-semibold text-white">{workflow.name}</h2>
        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
          workflow.state === 'completed' ? 'bg-green-900/30 text-green-400' :
          workflow.state === 'failed' ? 'bg-red-900/30 text-red-400' :
          'bg-blue-900/30 text-blue-400'
        }`}>
          {workflow.state}
        </span>
      </div>

      {/* DAG visualization (simplified — nodes with state colors) */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        <h3 className="text-sm font-medium text-gray-400 mb-4">DAG Steps</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {workflow.steps.map((step: WorkflowStep) => (
            <div
              key={step.name}
              className={`border rounded-lg p-4 ${stateColor(step.state)} cursor-pointer
                hover:ring-1 hover:ring-blue-500 transition-all`}
              role="button"
              tabIndex={0}
              aria-label={`Step ${step.name}: ${step.state}`}
            >
              <div className="flex items-center gap-2 mb-2">
                {stateIcon(step.state)}
                <span className="font-medium text-gray-200 text-sm">{step.name}</span>
              </div>
              <div className="text-xs text-gray-400">
                <p>Task: {step.task_name}</p>
                <p>State: {step.state}</p>
                {step.task_id && (
                  <p className="font-mono text-gray-500 mt-1 truncate">
                    {step.task_id}
                  </p>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
