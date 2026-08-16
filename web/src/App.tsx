import { Routes, Route, NavLink } from 'react-router-dom'
import { Activity, Users, ListTodo, GitBranch, Skull, Zap } from 'lucide-react'
import Overview from './pages/Overview'
import Workers from './pages/Workers'
import TaskDetailPage from './pages/TaskDetail'
import WorkflowView from './pages/WorkflowView'
import DLQPage from './pages/DLQ'
import ChaosPanel from './pages/ChaosPanel'

const navItems = [
  { to: '/', label: 'Overview', icon: Activity },
  { to: '/workers', label: 'Workers', icon: Users },
  { to: '/dlq', label: 'Dead Letters', icon: Skull },
  { to: '/workflows', label: 'Workflows', icon: GitBranch },
  { to: '/chaos', label: 'Chaos', icon: Zap },
]

export default function App() {
  return (
    <div className="flex h-screen">
      {/* Sidebar */}
      <nav className="w-56 bg-gray-900 border-r border-gray-800 flex flex-col" aria-label="Main navigation">
        <div className="p-4 border-b border-gray-800">
          <h1 className="text-lg font-semibold text-white flex items-center gap-2">
            <ListTodo className="w-5 h-5 text-blue-400" />
            DTQ Console
          </h1>
        </div>
        <ul className="flex-1 p-2 space-y-1">
          {navItems.map(({ to, label, icon: Icon }) => (
            <li key={to}>
              <NavLink
                to={to}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                    isActive
                      ? 'bg-blue-600/20 text-blue-400'
                      : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'
                  }`
                }
              >
                <Icon className="w-4 h-4" />
                {label}
              </NavLink>
            </li>
          ))}
        </ul>
        <div className="p-4 border-t border-gray-800 text-xs text-gray-500">
          v0.1.0
        </div>
      </nav>

      {/* Main content */}
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/workers" element={<Workers />} />
          <Route path="/tasks/:taskId" element={<TaskDetailPage />} />
          <Route path="/workflows/:workflowId" element={<WorkflowView />} />
          <Route path="/workflows" element={<WorkflowView />} />
          <Route path="/dlq" element={<DLQPage />} />
          <Route path="/chaos" element={<ChaosPanel />} />
        </Routes>
      </main>
    </div>
  )
}
