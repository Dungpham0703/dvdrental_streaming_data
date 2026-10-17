'use client';

import React, { useState, useRef, useEffect } from 'react';
import {
  Send,
  Bot,
  User,
  Code,
  ChevronDown,
  ChevronUp,
  Loader2,
  X,
  Plus,
  CheckCircle2,
  XCircle,
  Circle,
  Zap,
  Database,
  Search,
  BarChart3,
  Paperclip,
} from 'lucide-react';
import { DynamicChartRenderer } from './DynamicChartRenderer';

/* ============================
   Types
   ============================ */
interface WidgetData {
  title: string;
  sql: string;
  chartType: string;
  columns: string[];
  data: any[];
  execution_time_ms?: number;
  mode?: string;
  colSpan?: number;
  colorIntent?: string;
}

interface StreamingStep {
  id: string;
  label: string;
  status: 'running' | 'done' | 'error';
  detail?: string;
  timestamp?: number;
}

interface Message {
  id: string;
  sender: 'user' | 'bot';
  text: string;
  is_dashboard?: boolean;
  dashboard_title?: string;
  widgets?: WidgetData[];
  loading?: boolean;
  steps?: StreamingStep[];
  stepsDone?: number;
  stepsTotal?: number;
}

interface ChatInterfaceProps {
  isOpen: boolean;
  onClose: () => void;
  onDashboardReady?: (widgets: WidgetData[], title?: string, summary?: string) => void;
  selectedPrompt?: string;
  onClearSelectedPrompt?: () => void;
}

/* ============================
   Step Status Icon
   ============================ */
const StepStatusIcon: React.FC<{ status: string }> = ({ status }) => {
  if (status === 'done') return <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />;
  if (status === 'error') return <XCircle className="w-3.5 h-3.5 text-red-500" />;
  return <Loader2 className="w-3.5 h-3.5 text-brand-500 animate-spin" />;
};

const StepIcon: React.FC<{ label: string }> = ({ label }) => {
  const l = label.toLowerCase();
  if (l.includes('sql') || l.includes('sinh') || l.includes('generat'))
    return <Code className="w-3 h-3" />;
  if (l.includes('search') || l.includes('tìm') || l.includes('phân tích'))
    return <Search className="w-3 h-3" />;
  if (l.includes('athena') || l.includes('thực thi') || l.includes('execut'))
    return <Database className="w-3 h-3" />;
  if (l.includes('chart') || l.includes('biểu đồ') || l.includes('visual'))
    return <BarChart3 className="w-3 h-3" />;
  return <Zap className="w-3 h-3" />;
};

/* ============================
   Main Component
   ============================ */
export const ChatInterface: React.FC<ChatInterfaceProps> = ({
  isOpen,
  onClose,
  onDashboardReady,
  selectedPrompt,
  onClearSelectedPrompt,
}) => {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: 'welcome',
      sender: 'bot',
      text: 'Xin chào! Tôi là Tino AI Agent. Bạn có thể hỏi bất kỳ câu hỏi phân tích dữ liệu nào, hoặc yêu cầu tạo Dashboard tổng quan.',
    },
  ]);
  const [inputPrompt, setInputPrompt] = useState('');
  const [expandedSqlIds, setExpandedSqlIds] = useState<Record<string, boolean>>({});
  const [expandedStepIds, setExpandedStepIds] = useState<Record<string, boolean>>({});
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  useEffect(() => {
    if (selectedPrompt && isOpen) {
      handleSendPrompt(selectedPrompt);
      if (onClearSelectedPrompt) onClearSelectedPrompt();
    }
  }, [selectedPrompt]);

  const toggleSqlExpand = (id: string) => {
    setExpandedSqlIds((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleStepExpand = (id: string) => {
    setExpandedStepIds((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  /* ============================
     Streaming Query Handler
     ============================ */
  const handleSendPrompt = async (promptToSend?: string) => {
    const queryText = promptToSend || inputPrompt;
    if (!queryText.trim()) return;

    const userMsgId = Date.now().toString();
    const botMsgId = (Date.now() + 1).toString();

    const userMessage: Message = { id: userMsgId, sender: 'user', text: queryText };
    const botLoadingMessage: Message = {
      id: botMsgId,
      sender: 'bot',
      text: 'Đang xử lý...',
      loading: true,
      steps: [],
      stepsDone: 0,
      stepsTotal: 0,
    };

    setMessages((prev) => [...prev, userMessage, botLoadingMessage]);
    if (!promptToSend) setInputPrompt('');

    try {
      // Try streaming endpoint first
      const response = await fetch('http://localhost:8000/api/query/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: queryText }),
      });

      if (!response.ok) {
        // Fallback to non-streaming
        await handleNonStreamingQuery(queryText, botMsgId);
        return;
      }

      const reader = response.body?.getReader();
      if (!reader) {
        await handleNonStreamingQuery(queryText, botMsgId);
        return;
      }

      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const jsonStr = line.slice(6).trim();
          if (!jsonStr || jsonStr === '[DONE]') continue;

          try {
            const event = JSON.parse(jsonStr);

            if (event.type === 'step') {
              setMessages((prev) =>
                prev.map((msg) => {
                  if (msg.id !== botMsgId) return msg;
                  const existingSteps = [...(msg.steps || [])];
                  const stepIdx = existingSteps.findIndex((s) => s.id === event.step);

                  if (stepIdx >= 0) {
                    existingSteps[stepIdx] = {
                      ...existingSteps[stepIdx],
                      status: event.status,
                      detail: event.detail || existingSteps[stepIdx].detail,
                    };
                  } else {
                    existingSteps.push({
                      id: event.step,
                      label: event.message || event.step,
                      status: event.status,
                      detail: event.detail,
                      timestamp: Date.now(),
                    });
                  }

                  const doneCount = existingSteps.filter(
                    (s) => s.status === 'done' || s.status === 'error'
                  ).length;

                  return {
                    ...msg,
                    text: event.message || msg.text,
                    steps: existingSteps,
                    stepsDone: doneCount,
                    stepsTotal: existingSteps.length,
                  };
                })
              );
            }

            if (event.type === 'result') {
              const result = event.data;
              setMessages((prev) =>
                prev.map((msg) => {
                  if (msg.id !== botMsgId) return msg;
                  const finalSteps = [...(msg.steps || [])];
                  finalSteps.push({
                    id: 'complete',
                    label: 'Dashboard sẵn sàng',
                    status: 'done',
                    timestamp: Date.now(),
                  });
                  return {
                    ...msg,
                    text: result.summary || 'Đã phân tích xong.',
                    loading: false,
                    is_dashboard: result.is_dashboard,
                    dashboard_title: result.dashboard_title,
                    widgets: result.widgets,
                    steps: finalSteps,
                    stepsDone: finalSteps.length,
                    stepsTotal: finalSteps.length,
                  };
                })
              );

              // Push to canvas
              if (onDashboardReady && result.widgets?.length > 0) {
                onDashboardReady(result.widgets, result.dashboard_title, result.summary);
              }
            }

            if (event.type === 'error') {
              setMessages((prev) =>
                prev.map((msg) => {
                  if (msg.id !== botMsgId) return msg;
                  const errorSteps = [...(msg.steps || [])];
                  errorSteps.push({
                    id: 'error',
                    label: event.message || 'Lỗi xử lý',
                    status: 'error',
                    detail: event.detail,
                    timestamp: Date.now(),
                  });
                  return {
                    ...msg,
                    text: event.message || 'Đã xảy ra lỗi.',
                    loading: false,
                    steps: errorSteps,
                  };
                })
              );
            }
          } catch {
            // Ignore malformed JSON lines
          }
        }
      }
    } catch {
      // If streaming fails, try non-streaming
      await handleNonStreamingQuery(queryText, botMsgId);
    }
  };

  /** Fallback to non-streaming /api/query endpoint */
  const handleNonStreamingQuery = async (queryText: string, botMsgId: string) => {
    try {
      const fakeSteps: StreamingStep[] = [
        { id: 'analyze', label: 'Phân tích yêu cầu', status: 'done', timestamp: Date.now() },
        { id: 'generate', label: 'Sinh SQL via Gemini', status: 'running', timestamp: Date.now() },
      ];
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === botMsgId ? { ...msg, steps: fakeSteps, stepsDone: 1, stepsTotal: 2 } : msg
        )
      );

      const response = await fetch('http://localhost:8000/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: queryText }),
      });

      if (!response.ok) throw new Error(`HTTP Error ${response.status}`);

      const result = await response.json();

      const allSteps: StreamingStep[] = [
        { id: 'analyze', label: 'Phân tích yêu cầu', status: 'done', timestamp: Date.now() },
        { id: 'generate', label: 'Sinh SQL via Gemini', status: 'done', timestamp: Date.now() },
        { id: 'execute', label: `Thực thi ${result.widgets?.length || 0} truy vấn trên Athena`, status: 'done', timestamp: Date.now() },
        { id: 'complete', label: 'Dashboard sẵn sàng', status: 'done', timestamp: Date.now() },
      ];

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === botMsgId
            ? {
                id: botMsgId,
                sender: 'bot',
                text: result.summary || `Đã phân tích: "${queryText}"`,
                is_dashboard: result.is_dashboard,
                dashboard_title: result.dashboard_title,
                widgets: result.widgets || [],
                loading: false,
                steps: allSteps,
                stepsDone: allSteps.length,
                stepsTotal: allSteps.length,
              }
            : msg
        )
      );

      if (onDashboardReady && result.widgets?.length > 0) {
        onDashboardReady(result.widgets, result.dashboard_title, result.summary);
      }
    } catch (err: any) {
      const errorSteps: StreamingStep[] = [
        { id: 'analyze', label: 'Phân tích yêu cầu', status: 'done', timestamp: Date.now() },
        { id: 'error', label: `Lỗi: ${err.message || 'Không kết nối được Backend'}`, status: 'error', timestamp: Date.now() },
      ];

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === botMsgId
            ? {
                id: botMsgId,
                sender: 'bot',
                text: `Không thể hoàn tất: ${err.message || 'Lỗi kết nối API'}. Hãy đảm bảo FastAPI backend đang chạy tại port 8000.`,
                loading: false,
                steps: errorSteps,
              }
            : msg
        )
      );
    }
  };

  if (!isOpen) return null;

  return (
    <div
      id="chat-panel"
      className="w-[420px] h-[calc(100vh-57px)] border-l border-slate-200 bg-white flex flex-col animate-slide-in-right"
    >
      {/* Panel Header */}
      <div className="px-4 py-3 border-b border-slate-100 flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-2">
          <div className="status-dot success" />
          <span className="text-xs font-semibold text-slate-700">
            Trò chuyện trong không gian làm việc
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <button className="flex items-center gap-1 px-2.5 py-1 rounded-lg bg-brand-50 text-brand-500 text-[11px] font-semibold hover:bg-brand-100 transition-colors cursor-pointer">
            <Plus className="w-3 h-3" />
            New chat
          </button>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all cursor-pointer"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-4">
        {messages.map((msg) => (
          <div key={msg.id} className="animate-fade-in">
            {/* User Message */}
            {msg.sender === 'user' && (
              <div className="flex justify-end mb-3">
                <div className="max-w-[85%] flex items-start gap-2 flex-row-reverse">
                  <div className="w-7 h-7 rounded-lg bg-brand-500 text-white flex items-center justify-center flex-shrink-0">
                    <User className="w-3.5 h-3.5" />
                  </div>
                  <div className="px-3.5 py-2.5 rounded-2xl rounded-tr-sm bg-brand-500 text-white text-[13px] leading-relaxed shadow-sm">
                    {msg.text}
                  </div>
                </div>
              </div>
            )}

            {/* Bot Message */}
            {msg.sender === 'bot' && (
              <div className="space-y-2.5">
                {/* AI Label */}
                <div className="flex items-center gap-2">
                  <div className="w-7 h-7 rounded-lg bg-gradient-to-tr from-brand-50 to-blue-50 border border-brand-200 text-brand-500 flex items-center justify-center flex-shrink-0">
                    <Bot className="w-3.5 h-3.5" />
                  </div>
                  <span className="text-[11px] font-semibold text-slate-500">AI</span>
                  {msg.steps && msg.steps.length > 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="badge badge-brand flex items-center gap-1">
                        PLAN
                      </span>
                      {msg.stepsTotal && (
                        <span className={`badge ${msg.stepsDone === msg.stepsTotal ? 'badge-success' : 'badge-brand'}`}>
                          {msg.stepsDone}/{msg.stepsTotal}
                          {msg.stepsDone === msg.stepsTotal && ' ✓'}
                        </span>
                      )}
                    </div>
                  )}
                </div>

                {/* Reasoning Steps (Streaming) */}
                {msg.steps && msg.steps.length > 0 && (
                  <div className="ml-9 space-y-1">
                    {msg.steps.map((step) => (
                      <div key={step.id} className="animate-step-reveal">
                        <div className="flex items-center gap-2 py-1">
                          <StepStatusIcon status={step.status} />
                          <div className="flex items-center gap-1.5 px-2 py-0.5 rounded-md bg-slate-50 border border-slate-200">
                            <StepIcon label={step.label} />
                            <span className="text-[11px] font-mono font-medium text-slate-600">
                              {step.label.length > 40 ? step.label.substring(0, 40) + '...' : step.label}
                            </span>
                          </div>
                          {step.status === 'done' && (
                            <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />
                          )}
                        </div>
                        {step.detail && (
                          <button
                            onClick={() => toggleStepExpand(step.id)}
                            className="ml-6 text-[10px] text-slate-400 hover:text-brand-500 cursor-pointer"
                          >
                            {expandedStepIds[step.id] ? '▾ ẩn chi tiết' : '▸ chi tiết'}
                          </button>
                        )}
                        {step.detail && expandedStepIds[step.id] && (
                          <div className="ml-6 mt-1 text-[10px] text-slate-400 bg-slate-50 rounded-lg p-2 border border-slate-100 font-mono animate-fade-in">
                            {step.detail}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {/* Loading Indicator */}
                {msg.loading && (!msg.steps || msg.steps.length === 0) && (
                  <div className="ml-9 flex items-center gap-2 text-brand-500 text-[12px]">
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    <span>{msg.text}</span>
                  </div>
                )}

                {/* Message Text */}
                {!msg.loading && (
                  <div className="ml-9 px-3.5 py-2.5 rounded-2xl rounded-tl-sm bg-slate-50 border border-slate-100 text-[13px] text-slate-700 leading-relaxed">
                    {msg.text}
                  </div>
                )}

                {/* Inline Single Chart (for non-dashboard, compact view) */}
                {!msg.is_dashboard && msg.widgets && msg.widgets.length > 0 && !msg.loading && (
                  <div className="ml-9 space-y-2">
                    {msg.widgets.map((widget, wIdx) => (
                      <div key={wIdx}>
                        {/* SQL Toggle */}
                        <button
                          onClick={() => toggleSqlExpand(`${msg.id}-${wIdx}`)}
                          className="w-full flex items-center gap-2 px-3 py-2 rounded-lg bg-slate-50 border border-slate-200 text-[11px] text-slate-500 hover:text-brand-500 transition-colors cursor-pointer"
                        >
                          <Code className="w-3 h-3 text-brand-cyan" />
                          <span className="font-mono">{widget.title}</span>
                          <span className="ml-auto">
                            {expandedSqlIds[`${msg.id}-${wIdx}`] ? (
                              <ChevronUp className="w-3 h-3" />
                            ) : (
                              <ChevronDown className="w-3 h-3" />
                            )}
                          </span>
                        </button>
                        {expandedSqlIds[`${msg.id}-${wIdx}`] && (
                          <pre className="code-block mt-1 animate-fade-in">{widget.sql}</pre>
                        )}
                        {/* Compact chart in chat */}
                        <div className="mt-2">
                          <DynamicChartRenderer
                            data={widget.data}
                            columns={widget.columns}
                            chartType={widget.chartType}
                            executionTimeMs={widget.execution_time_ms}
                            mode={widget.mode}
                            compact={true}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Dashboard generated notice */}
                {msg.is_dashboard && msg.widgets && msg.widgets.length > 0 && !msg.loading && (
                  <div className="ml-9 px-3.5 py-2.5 rounded-xl bg-brand-50 border border-brand-200 text-[12px] text-brand-600 flex items-center gap-2">
                    <BarChart3 className="w-4 h-4" />
                    <span>
                      Dashboard <strong>{msg.dashboard_title}</strong> đã được tạo với{' '}
                      <strong>{msg.widgets.length} widgets</strong> — xem trên canvas chính.
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      {/* Input Bar */}
      <div className="px-4 py-3 border-t border-slate-100 flex-shrink-0">
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleSendPrompt();
          }}
          className="flex items-center gap-2 relative"
        >
          <button
            type="button"
            className="p-2 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all cursor-pointer"
          >
            <Paperclip className="w-4 h-4" />
          </button>
          <input
            id="chat-input"
            type="text"
            value={inputPrompt}
            onChange={(e) => setInputPrompt(e.target.value)}
            placeholder="Hỏi về file của bạn, dùng SQL, hoặc lưu ghi chú..."
            className="flex-1 glass-input text-slate-800 placeholder-slate-400 text-[13px] rounded-xl px-4 py-2.5 focus:outline-none transition-all"
          />
          <button
            id="btn-send"
            type="submit"
            disabled={!inputPrompt.trim()}
            className="p-2.5 rounded-xl bg-brand-500 hover:bg-brand-600 disabled:opacity-40 disabled:hover:bg-brand-500 text-white transition-all shadow-sm hover:shadow-brand cursor-pointer"
          >
            <Send className="w-4 h-4" />
          </button>
        </form>
      </div>
    </div>
  );
};
