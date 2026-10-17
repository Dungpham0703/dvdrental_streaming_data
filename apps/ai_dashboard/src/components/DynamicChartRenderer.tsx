'use client';

import React, { useState } from 'react';
import ReactECharts from 'echarts-for-react';
import {
  TrendingUp,
  BarChart2,
  PieChart as PieIcon,
  Table as TableIcon,
  DollarSign,
  Activity,
  AreaChart as AreaIcon,
} from 'lucide-react';

interface DynamicChartProps {
  data: any[];
  columns: string[];
  chartType: 'line' | 'bar' | 'pie' | 'stat' | 'table' | 'area' | string;
  executionTimeMs?: number;
  mode?: string;
  compact?: boolean;
  colorIntent?: string;
}

const COLOR_PALETTE = ['#6366F1', '#3B82F6', '#10B981', '#F59E0B', '#F43F5E', '#06B6D4', '#8B5CF6', '#EC4899'];
const GRADIENT_COLORS = [
  { start: '#6366F1', end: '#818CF8' },
  { start: '#3B82F6', end: '#60A5FA' },
  { start: '#10B981', end: '#34D399' },
  { start: '#F59E0B', end: '#FBBF24' },
];

/** Format large numbers: 1234567 → "1.23M", 1234 → "1,234" */
function formatNumber(val: number): string {
  if (Math.abs(val) >= 1_000_000_000) return (val / 1_000_000_000).toFixed(2) + 'B';
  if (Math.abs(val) >= 1_000_000) return (val / 1_000_000).toFixed(2) + 'M';
  if (Math.abs(val) >= 10_000) return (val / 1_000).toFixed(1) + 'K';
  return val.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function formatFullNumber(val: number): string {
  return val.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

// ECharts handles tooltips natively, no need for CustomTooltip

export const DynamicChartRenderer: React.FC<DynamicChartProps> = ({
  data,
  columns,
  chartType: initialChartType,
  executionTimeMs,
  mode,
  compact = false,
  colorIntent,
}) => {
  const [activeTab, setActiveTab] = useState<string>(initialChartType);

  // Dynamic palette based on colorIntent
  const currentPalette = React.useMemo(() => {
    if (!colorIntent) return COLOR_PALETTE;
    switch (colorIntent.toLowerCase()) {
      case 'positive': return ['#10B981', '#34D399', '#6EE7B7', '#A7F3D0'];
      case 'negative':
      case 'warning': return ['#EF4444', '#F87171', '#FCA5A5', '#FECACA'];
      case 'indigo': return ['#6366F1', '#818CF8', '#A5B4FC', '#C7D2FE'];
      case 'emerald': return ['#10B981', '#34D399', '#6EE7B7', '#A7F3D0'];
      case 'sunset': return ['#F59E0B', '#FBBF24', '#FCD34D', '#FDE68A'];
      default: return COLOR_PALETTE;
    }
  }, [colorIntent]);

  if (!data || data.length === 0) {
    return (
      <div className="glass-card rounded-2xl p-8 text-center text-slate-400 text-sm">
        Không có dữ liệu trả về từ truy vấn.
      </div>
    );
  }

  const sampleRow = data[0];
  const keys = Object.keys(sampleRow);

  // Helper to identify ID columns that shouldn't be plotted as metrics
  const isIdCol = (k: string) => {
    const lk = k.toLowerCase();
    return (lk === 'id' || lk.endsWith('_id') || lk.endsWith('id')) && !lk.includes('count') && !lk.includes('total');
  };

  const rawNumericKeys = keys.filter((k) => typeof sampleRow[k] === 'number');
  // Filter out ID columns unless they are the only numeric keys
  const numericKeys = rawNumericKeys.filter((k) => !isIdCol(k));
  const effectiveNumericKeys = numericKeys.length > 0 ? numericKeys : rawNumericKeys;

  const nonNumericKeys = keys.filter((k) => typeof sampleRow[k] !== 'number' || isIdCol(k));
  const primaryXKey = nonNumericKeys.find(k => !isIdCol(k)) || nonNumericKeys[0] || keys[0];
  const primaryYKey = effectiveNumericKeys[0] || keys[1] || keys[0];

  const chartHeight = compact ? 220 : 320;

  // === STAT CARD ===
  if (activeTab === 'stat' || (data.length === 1 && keys.length <= 4 && activeTab !== 'table')) {
    const gridColsClass =
      keys.length === 1
        ? 'grid-cols-1'
        : keys.length === 2
        ? 'grid-cols-1 sm:grid-cols-2'
        : keys.length === 3
        ? 'grid-cols-1 sm:grid-cols-3'
        : 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-4';

    return (
      <div className={`grid ${gridColsClass} gap-4 w-full`}>
        {keys.map((key, idx) => {
          const value = sampleRow[key];
          const isNumeric = typeof value === 'number';
          return (
            <div
              key={key}
              className="glass-card rounded-2xl p-4 bg-white border border-slate-200/80 hover:border-brand-200 shadow-sm hover:shadow-md transition-all relative overflow-hidden group"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-[11px] font-bold text-slate-400 uppercase tracking-wider">
                  {key.replace(/_/g, ' ')}
                </span>
                <div
                  className="p-1.5 rounded-lg text-slate-400 group-hover:text-brand-500 transition-colors"
                  style={{
                    backgroundColor: `${COLOR_PALETTE[idx % COLOR_PALETTE.length]}10`,
                  }}
                >
                  <DollarSign className="w-3.5 h-3.5" />
                </div>
              </div>
              <div className="text-xl sm:text-2xl font-black text-slate-900 tracking-tight">
                {isNumeric ? formatNumber(value) : value}
              </div>
              {isNumeric && (
                <div className="text-[10px] text-slate-400 font-mono mt-1">
                  {formatFullNumber(value)}
                </div>
              )}
            </div>
          );
        })}
      </div>
    );
  }

  // Determine allowed chart types dynamically based on data shape
  let allowedKeys = ['table'];
  if (numericKeys.length > 0) {
    if (nonNumericKeys.length === 0) {
      allowedKeys = ['bar', 'table'];
    } else {
      const xIsTime = primaryXKey.toLowerCase().match(/date|time|month|year|day|ngay|thang|nam/);
      if (xIsTime) {
        allowedKeys = ['line', 'area', 'bar', 'table'];
      } else {
        // If categorical and not too many groups, allow pie/donut
        if (numericKeys.length === 1 && data.length <= 6) {
          allowedKeys = ['bar', 'horizontal_bar', 'pie', 'donut', 'table'];
        } else {
          allowedKeys = ['bar', 'horizontal_bar', 'table'];
        }
      }
    }
    // If multiple numeric keys and no categorical, allow scatter
    if (numericKeys.length >= 2 && nonNumericKeys.length === 0) {
      allowedKeys.push('scatter');
    }
  }

  const tabs = [
    { key: 'line', label: 'Line', icon: TrendingUp },
    { key: 'area', label: 'Area', icon: AreaIcon },
    { key: 'bar', label: 'Bar', icon: BarChart2 },
    { key: 'horizontal_bar', label: 'H-Bar', icon: BarChart2 },
    { key: 'pie', label: 'Pie', icon: PieIcon },
    { key: 'donut', label: 'Donut', icon: PieIcon },
    { key: 'scatter', label: 'Scatter', icon: Activity },
    { key: 'table', label: 'Bảng', icon: TableIcon },
  ].filter((t) => allowedKeys.includes(t.key));

  const currentTab = allowedKeys.includes(activeTab) ? activeTab : allowedKeys[0];

  return (
    <div className="glass-card rounded-2xl p-4 sm:p-5 flex flex-col h-full w-full">
      {/* Header & Controls */}
      <div className="flex flex-wrap items-center justify-between gap-3 mb-4 shrink-0">
        <div className="flex items-center gap-2">
          <div className="status-dot success" />
          <h3 className="text-xs font-semibold text-slate-600 uppercase tracking-wider">
            {data.length} dòng dữ liệu
          </h3>
          {executionTimeMs && (
            <span className="badge badge-brand">{executionTimeMs}ms</span>
          )}
        </div>

        {/* View Switcher */}
        <div className="flex items-center gap-0.5 bg-slate-100 p-0.5 rounded-xl">
          {tabs.map((tab) => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-[11px] font-medium transition-all cursor-pointer ${
                  currentTab === tab.key
                    ? 'bg-white text-brand-500 shadow-sm'
                    : 'text-slate-400 hover:text-slate-600'
                }`}
              >
                <Icon className="w-3 h-3" />
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Chart Canvas */}
      <div className="w-full flex-1 min-h-0 relative" style={!compact ? { minHeight: 250 } : undefined}>
        {['line', 'area', 'bar', 'horizontal_bar', 'pie', 'donut', 'scatter'].includes(currentTab) && (
          <ReactECharts
            option={(() => {
              const commonOption = {
                color: currentPalette,
                tooltip: {
                  trigger: ['pie', 'donut', 'scatter'].includes(currentTab) ? 'item' : 'axis',
                  textStyle: { fontSize: 11 },
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  borderColor: '#e2e8f0',
                  formatter: ['pie', 'donut'].includes(currentTab) ? '{b}: <b>{c}</b> ({d}%)' : undefined
                },
                legend: {
                  bottom: 0,
                  textStyle: { fontSize: 11, color: '#64748b' },
                  type: 'scroll',
                  icon: 'circle'
                },
                grid: {
                  top: 30,
                  right: 20,
                  bottom: 30,
                  left: 10,
                  containLabel: true,
                }
              };

              if (['pie', 'donut'].includes(currentTab)) {
                const isDonut = currentTab === 'donut';
                return {
                  ...commonOption,
                  series: [
                    {
                      type: 'pie',
                      radius: isDonut ? (compact ? ['35%', '70%'] : ['40%', '75%']) : (compact ? '70%' : '75%'),
                      avoidLabelOverlap: true,
                      itemStyle: {
                        borderRadius: 4,
                        borderColor: '#fff',
                        borderWidth: 2
                      },
                      label: {
                        show: true,
                        formatter: '{b}\\n{d}%',
                        color: '#64748b',
                        fontSize: 10
                      },
                      data: data.map(d => ({ name: String(d[primaryXKey]), value: d[primaryYKey] }))
                    }
                  ]
                };
              }

              if (currentTab === 'scatter') {
                return {
                  ...commonOption,
                  xAxis: {
                    type: 'value',
                    name: numericKeys[0],
                    nameLocation: 'middle',
                    nameGap: 25,
                  },
                  yAxis: {
                    type: 'value',
                    name: numericKeys[1] || numericKeys[0],
                  },
                  series: [
                    {
                      type: 'scatter',
                      symbolSize: 10,
                      data: data.map(d => [d[numericKeys[0]], d[numericKeys[1] || numericKeys[0]]]),
                    }
                  ]
                };
              }

              const xAxisData = data.map(d => String(d[primaryXKey]));

              const series = numericKeys.map((numKey, index) => {
                const baseSeries: any = {
                  name: numKey.replace(/_/g, ' '),
                  data: data.map(d => d[numKey]),
                  type: ['bar', 'horizontal_bar'].includes(currentTab) ? 'bar' : 'line',
                  smooth: true,
                  symbolSize: 6,
                };

                if (['bar', 'horizontal_bar'].includes(currentTab)) {
                  baseSeries.itemStyle = {
                    borderRadius: currentTab === 'horizontal_bar' ? [0, 4, 4, 0] : [4, 4, 0, 0]
                  };
                  if (numericKeys.length === 1) {
                     baseSeries.colorBy = 'data'; // Auto colors if single metric
                  }
                  baseSeries.barMaxWidth = 50;
                }

                if (currentTab === 'area') {
                  baseSeries.areaStyle = {
                    color: {
                      type: 'linear',
                      x: 0, y: 0, x2: 0, y2: 1,
                      colorStops: [
                        { offset: 0, color: GRADIENT_COLORS[index % GRADIENT_COLORS.length].start },
                        { offset: 1, color: GRADIENT_COLORS[index % GRADIENT_COLORS.length].end + '10' } // very transparent end
                      ]
                    }
                  };
                  baseSeries.lineStyle = { width: 2, color: GRADIENT_COLORS[index % GRADIENT_COLORS.length].start };
                  baseSeries.itemStyle = { color: GRADIENT_COLORS[index % GRADIENT_COLORS.length].start };
                }

                return baseSeries;
              });

              if (currentTab === 'horizontal_bar') {
                return {
                  ...commonOption,
                  yAxis: {
                    type: 'category',
                    data: xAxisData,
                    axisLine: { lineStyle: { color: '#cbd5e1' } },
                    axisLabel: { color: '#64748b', fontSize: 10, hideOverlap: true }
                  },
                  xAxis: {
                    type: 'value',
                    axisLine: { show: false },
                    axisTick: { show: false },
                    splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
                    axisLabel: { color: '#64748b', fontSize: 10, formatter: (val: number) => formatNumber(val) }
                  },
                  series
                };
              }

              return {
                ...commonOption,
                xAxis: {
                  type: 'category',
                  data: xAxisData,
                  axisLine: { lineStyle: { color: '#cbd5e1' } },
                  axisLabel: { color: '#64748b', fontSize: 10, hideOverlap: true }
                },
                yAxis: {
                  type: 'value',
                  axisLine: { show: false },
                  axisTick: { show: false },
                  splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
                  axisLabel: { color: '#64748b', fontSize: 10, formatter: (val: number) => formatNumber(val) }
                },
                series
              };
            })()}
            style={{ height: '100%', width: '100%' }}
            opts={{ renderer: 'canvas' }}
          />
        )}

        {currentTab === 'table' && (
          <div className="overflow-auto h-full rounded-xl border border-slate-200">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 text-slate-500 uppercase font-semibold sticky top-0 border-b border-slate-200">
                <tr>
                  {keys.map((key) => (
                    <th key={key} className="px-4 py-3 whitespace-nowrap">
                      {key.replace(/_/g, ' ')}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.map((row, idx) => (
                  <tr key={idx} className="hover:bg-brand-50/50 transition-colors">
                    {keys.map((key) => (
                      <td key={key} className="px-4 py-2.5 text-slate-700 font-mono text-[11px] whitespace-nowrap">
                        {typeof row[key] === 'number'
                          ? formatFullNumber(row[key])
                          : String(row[key])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};
