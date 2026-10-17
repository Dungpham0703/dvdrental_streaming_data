'use client';

import React from 'react';
import { DashboardGrid } from './DashboardGrid';
import { Sparkles, BarChart3, PieChart, TrendingUp, Table, ArrowRight } from 'lucide-react';

interface WidgetData {
  title: string;
  sql: string;
  chartType: string;
  columns: string[];
  data: any[];
  execution_time_ms?: number;
  mode?: string;
}

interface DashboardCanvasProps {
  dashboardTitle?: string;
  dashboardSummary?: string;
  widgets: WidgetData[];
  onSelectPrompt?: (prompt: string) => void;
}

const QUICK_ACTIONS = [
  {
    icon: BarChart3,
    title: 'Executive Dashboard',
    desc: 'Tổng quan doanh thu, khách hàng, và biểu đồ đa chiều',
    prompt: 'Tạo cho tôi Executive Dashboard tổng quan doanh thu và khách hàng',
    color: '#6366F1',
  },
  {
    icon: TrendingUp,
    title: 'Phân tích xu hướng',
    desc: 'Trend doanh thu theo thời gian',
    prompt: 'Trend doanh thu 3 tháng gần nhất',
    color: '#3B82F6',
  },
  {
    icon: PieChart,
    title: 'Phân bổ theo thể loại',
    desc: 'Tỷ trọng doanh thu theo phim',
    prompt: 'Top 5 thể loại phim có doanh thu cao nhất',
    color: '#10B981',
  },
  {
    icon: Table,
    title: 'Khách hàng VIP',
    desc: 'Top khách hàng chi tiêu cao nhất',
    prompt: 'Liệt kê top 10 khách hàng chi tiêu nhiều nhất',
    color: '#F59E0B',
  },
];

export const DashboardCanvas: React.FC<DashboardCanvasProps> = ({
  dashboardTitle,
  dashboardSummary,
  widgets,
  onSelectPrompt,
}) => {
  // === Empty State ===
  if (!widgets || widgets.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center p-8 bg-gradient-canvas bg-dots min-h-[calc(100vh-57px)]">
        <div className="max-w-2xl w-full space-y-8 animate-fade-in-up">
          {/* Hero */}
          <div className="text-center space-y-4">
            <div className="inline-flex p-4 rounded-2xl bg-gradient-to-tr from-brand-500 to-brand-cyan shadow-brand-lg">
              <Sparkles className="w-10 h-10 text-white" />
            </div>
            <h1 className="text-3xl font-extrabold text-slate-900 tracking-tight">
              Chào mừng đến{' '}
              <span className="gradient-text">Tino Data</span>
            </h1>
            <p className="text-sm text-slate-500 max-w-md mx-auto leading-relaxed">
              Hỏi bằng ngôn ngữ tự nhiên, AI sẽ tự động sinh SQL và tạo Dashboard trực quan từ AWS Athena Lakehouse.
            </p>
          </div>

          {/* Quick Actions Grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {QUICK_ACTIONS.map((action, idx) => {
              const Icon = action.icon;
              return (
                <button
                  key={idx}
                  id={`canvas-action-${idx}`}
                  onClick={() => onSelectPrompt?.(action.prompt)}
                  className="glass-card rounded-2xl p-4 text-left group cursor-pointer hover:shadow-lg transition-all"
                >
                  <div className="flex items-start gap-3">
                    <div
                      className="p-2.5 rounded-xl transition-all group-hover:scale-110"
                      style={{
                        backgroundColor: `${action.color}12`,
                        color: action.color,
                      }}
                    >
                      <Icon className="w-5 h-5" />
                    </div>
                    <div className="flex-1">
                      <div className="text-sm font-semibold text-slate-700 group-hover:text-brand-500 transition-colors flex items-center gap-1">
                        {action.title}
                        <ArrowRight className="w-3.5 h-3.5 opacity-0 -translate-x-1 group-hover:opacity-100 group-hover:translate-x-0 transition-all" />
                      </div>
                      <div className="text-[11px] text-slate-400 mt-0.5">
                        {action.desc}
                      </div>
                    </div>
                  </div>
                </button>
              );
            })}
          </div>

          {/* Hint */}
          <p className="text-center text-xs text-slate-400">
            Hoặc gõ câu hỏi trong panel{' '}
            <span className="font-semibold text-brand-500">Hội thoại</span> bên phải
          </p>
        </div>
      </div>
    );
  }

  // === Dashboard View ===
  return (
    <div className="flex-1 overflow-y-auto p-6 bg-gradient-canvas bg-grid min-h-[calc(100vh-57px)]">
      <div className="max-w-6xl mx-auto">
        <DashboardGrid
          title={dashboardTitle}
          summary={dashboardSummary}
          widgets={widgets}
        />
      </div>
    </div>
  );
};
