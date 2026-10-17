'use client';

import React, { useState, useEffect, useRef } from 'react';
import { DynamicChartRenderer } from './DynamicChartRenderer';
import { LayoutDashboard, Code, ChevronDown, ChevronUp, Sparkles, Filter, Calendar, MapPin, GripHorizontal, Maximize, Minimize, AlertTriangle, FileText, Info } from 'lucide-react';
import { ResponsiveGridLayout, useContainerWidth, Layout } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

export interface WidgetData {
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

interface DashboardGridProps {
  title?: string;
  summary?: string;
  widgets: WidgetData[];
}

export const DashboardGrid: React.FC<DashboardGridProps> = ({
  title = 'Tino Data Analytics Dashboard',
  summary,
  widgets,
}) => {
  const [expandedSqlIdxs, setExpandedSqlIdxs] = useState<Record<number, boolean>>({});
  const [isFullscreen, setIsFullscreen] = useState(false);
  const dashboardRef = useRef<HTMLDivElement>(null);

  const toggleSql = (idx: number) => {
    setExpandedSqlIdxs((prev) => ({ ...prev, [idx]: !prev[idx] }));
  };

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      dashboardRef.current?.requestFullscreen().catch(err => {
        console.error(`Error attempting to enable fullscreen: ${err.message}`);
      });
    } else {
      document.exitFullscreen();
    }
  };

  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };
    document.addEventListener('fullscreenchange', handleFullscreenChange);
    return () => document.removeEventListener('fullscreenchange', handleFullscreenChange);
  }, []);

  const getColSpanW = (w: WidgetData): number => {
    if (w.chartType === 'textarea' || w.chartType === 'note' || w.chartType === 'text' || w.chartType === 'alert') return 12; // 12 is max width in our grid cols
    return w.colSpan || (w.chartType === 'stat' ? 3 : w.chartType === 'pie' ? 5 : w.chartType === 'line' || w.chartType === 'area' ? 7 : 12);
  };

  const getColSpanH = (w: WidgetData): number => {
    if (w.chartType === 'stat') return 2;
    if (w.chartType === 'textarea' || w.chartType === 'text') return 3;
    if (w.chartType === 'note' || w.chartType === 'alert') return 2;
    return 6;
  };

  // Generate default layout with accurate row height tracking
  const generateLayout = (): Layout[] => {
    let currentY = 0;
    let currentX = 0;
    let currentRowMaxH = 0;

    return widgets.map((widget, i) => {
      const w = getColSpanW(widget);
      const h = getColSpanH(widget);

      if (currentX + w > 12) {
        currentX = 0;
        currentY += currentRowMaxH || 2;
        currentRowMaxH = 0;
      }

      currentRowMaxH = Math.max(currentRowMaxH, h);

      const layoutItem = {
        i: i.toString(),
        x: currentX,
        y: currentY,
        w: w,
        h: h,
        minW: 3,
        minH: 2
      };

      currentX += w;
      return layoutItem;
    });
  };

  const [layouts, setLayouts] = useState<{ lg: Layout[] }>({ lg: [] });
  const [isMounted, setIsMounted] = useState(false);

  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Regenerate layout when widgets change
  useEffect(() => {
    if (widgets.length > 0) {
      setLayouts({ lg: generateLayout() });
    }
  }, [widgets]);

  const onLayoutChange = (layout: Layout[], allLayouts: any) => {
    if (!allLayouts?.lg) return;
    setLayouts((prev) => {
      if (!prev.lg || prev.lg.length !== allLayouts.lg.length) return allLayouts;

      // Prevent infinite loops caused by JSON.stringify failing due to internal properties
      const isSame = prev.lg.every((item) => {
        const other = allLayouts.lg.find((o: any) => o.i === item.i);
        return other && item.x === other.x && item.y === other.y && item.w === other.w && item.h === other.h;
      });

      if (isSame) {
        return prev;
      }
      return allLayouts;
    });
  };

  // --- AUTO GLOBAL FILTERS LOGIC ---
  const [activeFilters, setActiveFilters] = useState<Record<string, string>>({});

  // 1. Discover potential filters from all data
  const filterOptions = React.useMemo(() => {
    const options: Record<string, Set<string>> = {};
    widgets.forEach(w => {
      if (!w.data || w.data.length === 0) return;
      const keys = Object.keys(w.data[0]);
      keys.forEach(k => {
        // Only consider non-numeric keys as potential filters
        if (typeof w.data[0][k] === 'string' || typeof w.data[0][k] === 'boolean') {
          if (!options[k]) options[k] = new Set();
          w.data.forEach((row: any) => options[k].add(String(row[k])));
        }
      });
    });

    // 2. Filter out keys that have too many unique values (e.g., > 30) or just 1 value
    const validFilters: Record<string, string[]> = {};
    Object.keys(options).forEach(k => {
      const size = options[k].size;
      if (size > 1 && size <= 30) {
        validFilters[k] = Array.from(options[k]).sort();
      }
    });
    return validFilters;
  }, [widgets]);

  // 3. Apply active filters to widgets
  const filteredWidgets = React.useMemo(() => {
    if (Object.keys(activeFilters).length === 0) return widgets;

    return widgets.map(w => {
      if (!w.data) return w;
      const filteredData = w.data.filter((row: any) => {
        for (const [key, val] of Object.entries(activeFilters)) {
          if (val === '') continue; // Empty means "All"
          // If the widget has this column, filter by it. If it doesn't, ignore this filter for this widget.
          if (row[key] !== undefined && String(row[key]) !== val) {
            return false;
          }
        }
        return true;
      });
      return { ...w, data: filteredData };
    });
  }, [widgets, activeFilters]);

  const handleFilterChange = (key: string, value: string) => {
    setActiveFilters(prev => {
      const next = { ...prev };
      if (value === '') delete next[key];
      else next[key] = value;
      return next;
    });
  };

  const { width, containerRef, mounted } = useContainerWidth();
  const hasSimulatedData = widgets.some(w => w.mode === 'simulated');

  useEffect(() => {
    if (hasSimulatedData) {
      console.warn("⚠️ Cảnh báo: Dữ liệu đang hiển thị là dữ liệu giả lập (Simulated Data). Không kết nối được Database thật.");
    }
  }, [hasSimulatedData]);

  return (
    <div ref={dashboardRef} className={`space-y-5 w-full animate-fade-in-up ${isFullscreen ? 'p-6 bg-slate-50 overflow-y-auto h-screen' : ''}`}>
      {/* Dashboard Header Banner */}
      <div className="glass-card rounded-2xl p-5 bg-gradient-to-r from-brand-50 via-white to-blue-50 relative overflow-hidden">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="p-3 rounded-xl bg-brand-500/10 text-brand-500 border border-brand-200">
              <LayoutDashboard className="w-5 h-5" />
            </div>
            <div>
              <h2 className="text-base font-bold text-slate-900 tracking-tight flex items-center gap-2">
                {title}
                <Sparkles className="w-4 h-4 text-brand-accent" />
              </h2>
              <p className="text-[11px] text-slate-400 mt-0.5 max-w-md">
                {summary || 'Dashboard tổng quan tích hợp các chỉ số phân tích từ AWS Athena Data Mart.'}
              </p>
            </div>
          </div>
          <div className="hidden sm:flex items-center gap-2">
            <span className="badge badge-brand">{widgets.length} widgets</span>
            <span className="badge badge-success">AI Dynamic Layout</span>
            <button
              onClick={toggleFullscreen}
              className="ml-2 p-2 rounded-lg bg-white border border-slate-200 text-slate-500 hover:text-brand-500 hover:border-brand-300 transition-colors shadow-sm"
              title="Phóng to toàn màn hình"
            >
              {isFullscreen ? <Minimize className="w-4 h-4" /> : <Maximize className="w-4 h-4" />}
            </button>
          </div>
        </div>

        {hasSimulatedData && (
          <div className="mt-4 flex items-center gap-2 bg-yellow-50 border border-yellow-200 text-yellow-800 text-xs px-4 py-2.5 rounded-lg animate-pulse">
            <AlertTriangle className="w-4 h-4 text-yellow-600" />
            <span><strong>Cảnh báo:</strong> Đang hiển thị dữ liệu giả lập (Simulated Data) do không kết nối được Database.</span>
          </div>
        )}

        {/* Filter Bar */}
        <div className="flex flex-wrap items-center gap-3 mt-4 pt-4 border-t border-slate-200/60">
          <div className="flex items-center gap-1.5 text-[11px] text-slate-500 mr-2">
            <Filter className="w-3 h-3" />
            <span className="font-bold tracking-wider">SMART FILTERS</span>
          </div>
          {Object.keys(filterOptions).length === 0 && (
            <span className="text-[11px] text-slate-400 italic">Không có cột dữ liệu nào để phân nhóm.</span>
          )}
          {Object.entries(filterOptions).map(([key, values]) => {
            const isTime = key.toLowerCase().match(/thang|nam|date|month|year/);
            return (
              <div key={key} className="flex items-center relative group">
                <div className="absolute left-3 z-10 pointer-events-none">
                  {isTime ? <Calendar className="w-3 h-3 text-brand-500" /> : <MapPin className="w-3 h-3 text-brand-500" />}
                </div>
                <select
                  className="pl-8 pr-8 py-1.5 rounded-lg bg-white border border-slate-200 text-[11px] font-semibold text-slate-700 cursor-pointer hover:border-brand-300 focus:border-brand-500 focus:ring-1 focus:ring-brand-500 transition-all outline-none appearance-none shadow-sm min-w-[140px]"
                  value={activeFilters[key] || ''}
                  onChange={(e) => handleFilterChange(key, e.target.value)}
                >
                  <option value="">Tất cả {key.replace(/_/g, ' ')}</option>
                  {values.map(v => (
                    <option key={v} value={v}>{v}</option>
                  ))}
                </select>
                <ChevronDown className="w-3 h-3 text-slate-400 absolute right-3 pointer-events-none group-hover:text-brand-500 transition-colors" />
              </div>
            );
          })}
        </div>
      </div>

      {/* Drag & Drop Grid */}
      <div ref={containerRef}>
        {mounted && width > 0 ? (
          <ResponsiveGridLayout
            width={width}
            className="layout -mx-2 transition-all duration-300"
            layouts={layouts}
            breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
            cols={{ lg: 12, md: 12, sm: 12, xs: 1, xxs: 1 }}
            rowHeight={60}
            onLayoutChange={onLayoutChange}
            draggableHandle=".drag-handle"
            margin={[20, 20]}
            isBounded={true}
          >
          {filteredWidgets.map((widget, idx) => {
            const isStat = widget.chartType === 'stat';

            if (isStat) {
              return (
                <div key={idx.toString()} className="bg-white rounded-2xl border border-slate-100 shadow-sm relative group overflow-hidden flex flex-col justify-center p-4">
                  <div className="drag-handle cursor-move absolute top-2 right-2 p-1 bg-slate-50 rounded z-10 opacity-0 group-hover:opacity-100 transition-opacity">
                     <GripHorizontal className="w-4 h-4 text-slate-400" />
                  </div>
                  <DynamicChartRenderer
                    data={widget.data}
                    columns={widget.columns}
                    chartType="stat"
                    executionTimeMs={widget.execution_time_ms}
                    mode={widget.mode}
                  />
                </div>
              );
            }

            const isTextNode = ['textarea', 'text', 'note', 'alert'].includes(widget.chartType);
            if (isTextNode) {
              const textContent = widget.data?.[0]?.content || '';
              const isNote = ['note', 'alert'].includes(widget.chartType);

              return (
                <div key={idx.toString()} className={`rounded-2xl p-5 relative group overflow-hidden flex flex-col ${isNote ? 'bg-amber-50 border border-amber-200' : 'bg-transparent'}`}>
                  <div className="drag-handle cursor-move absolute top-2 right-2 p-1 bg-slate-100 rounded z-10 opacity-0 group-hover:opacity-100 transition-opacity">
                     <GripHorizontal className="w-4 h-4 text-slate-400" />
                  </div>

                  {isNote && (
                    <div className="flex items-center gap-2 mb-2 text-amber-700">
                      <Info className="w-4 h-4" />
                      <span className="text-xs font-bold uppercase tracking-wider">Lưu ý phân tích</span>
                    </div>
                  )}

                  <div className={`prose prose-sm max-w-none whitespace-pre-wrap ${isNote ? 'text-amber-900' : 'text-slate-700'}`}>
                    {textContent}
                  </div>
                </div>
              );
            }

            return (
              <div key={idx.toString()} className="glass-card rounded-2xl p-5 space-y-3 flex flex-col relative group">
                <div className="drag-handle cursor-move absolute top-4 right-1/2 translate-x-1/2 p-1 bg-slate-100 rounded z-10 opacity-0 group-hover:opacity-100 transition-opacity">
                  <GripHorizontal className="w-4 h-4 text-slate-400" />
                </div>

                <div className="flex items-center justify-between">
                  <h4 className="text-xs font-bold text-slate-800 uppercase tracking-wider flex items-center gap-2">
                    <div
                      className={`w-2 h-2 rounded-full ${
                        widget.chartType === 'pie'
                          ? 'bg-brand-500'
                          : widget.chartType === 'line'
                          ? 'bg-brand-cyan'
                          : 'bg-brand-emerald'
                      }`}
                    />
                    {widget.title}
                  </h4>
                  <button
                    onClick={() => toggleSql(idx)}
                    className="flex items-center gap-1 text-[10px] text-slate-400 hover:text-brand-500 transition-colors font-mono cursor-pointer z-10 relative"
                  >
                    <Code className="w-3 h-3" /> SQL
                    {expandedSqlIdxs[idx] ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                  </button>
                </div>

                {expandedSqlIdxs[idx] && <pre className="code-block animate-fade-in text-[9px] max-h-32 overflow-y-auto relative z-10">{widget.sql}</pre>}

                <div className="flex-1 min-h-0 relative z-0">
                  <DynamicChartRenderer
                    data={widget.data}
                    columns={widget.columns}
                    chartType={widget.chartType}
                    executionTimeMs={widget.execution_time_ms}
                    mode={widget.mode}
                    compact={widget.colSpan ? widget.colSpan <= 6 : widget.chartType === 'pie'}
                  />
                </div>
              </div>
            );
          })}
        </ResponsiveGridLayout>
        ) : (
          <div className="h-64 flex items-center justify-center text-slate-400 text-sm">Đang tải khung hiển thị...</div>
        )}
      </div>
    </div>
  );
};
