'use client';

import React, { useState } from 'react';
import {
  MessageSquarePlus,
  Search,
  ChevronRight,
  ChevronDown,
  Table,
  Database,
  Layers,
  Zap,
  LayoutDashboard,
  FolderOpen,
  FileText,
  Upload,
  Compass,
  Star,
} from 'lucide-react';

interface SidebarProps {
  onSelectPrompt: (prompt: string) => void;
  isOpen?: boolean;
}

const SAMPLE_PROMPTS = [
  {
    icon: LayoutDashboard,
    title: 'Executive Dashboard Tổng Quan',
    subtitle: 'Tự động tạo 4 biểu đồ đa chiều (KPI, Line, Pie, Table)',
    prompt: 'Tạo cho tôi Executive Dashboard tổng quan doanh thu và khách hàng',
    highlight: true,
  },
  {
    icon: Zap,
    title: 'Trend doanh thu 3 tháng gần nhất',
    subtitle: 'Tự động render biểu đồ đường (LineChart)',
    prompt: 'Trend doanh thu 3 tháng gần nhất',
  },
  {
    icon: Compass,
    title: 'Top 5 thể loại phim doanh thu cao nhất',
    subtitle: 'Tự động render biểu đồ tròn (PieChart)',
    prompt: 'Top 5 thể loại phim có doanh thu cao nhất',
  },
  {
    icon: Layers,
    title: 'Tổng doanh thu & tổng giao dịch',
    subtitle: 'Tự động render chỉ số KPI (Stat Card)',
    prompt: 'Cho tôi biết tổng doanh thu và tổng số lượt thanh toán',
  },
  {
    icon: Table,
    title: 'Danh sách Top 10 khách hàng VIP',
    subtitle: 'Tự động render bảng dữ liệu SCD2 (Data Table)',
    prompt: 'Liệt kê top 10 khách hàng chi tiêu nhiều nhất',
  },
];

interface TableNode {
  name: string;
  type: 'Fact' | 'Dimension' | 'SCD2';
  desc: string;
  columns: string[];
}

const GOLD_TABLES: TableNode[] = [
  {
    name: 'fct_payment',
    type: 'Fact',
    desc: 'Thanh toán & doanh thu USD (9 cột)',
    columns: ['payment_id', 'customer_id', 'staff_id', 'rental_id', 'film_id', 'store_id', 'amount', 'payment_date', 'cdc_capture_at'],
  },
  {
    name: 'fct_rental',
    type: 'Fact',
    desc: 'Lượt thuê đĩa & thời gian thuê (9 cột)',
    columns: ['rental_id', 'rental_date', 'return_date', 'customer_id', 'film_id', 'store_id', 'staff_id', 'rental_duration_days', 'cdc_capture_at'],
  },
  {
    name: 'dim_customer',
    type: 'SCD2',
    desc: 'Thông tin khách hàng SCD2 (19 cột)',
    columns: ['customer_key', 'customer_id', 'first_name', 'last_name', 'email', 'store_id', 'is_active', 'active', 'create_date', 'address', 'address2', 'district', 'city_name', 'country_name', 'postal_code', 'phone', 'valid_from', 'valid_to', 'is_current'],
  },
  {
    name: 'dim_film',
    type: 'Dimension',
    desc: 'Danh mục phim & thể loại (13 cột)',
    columns: ['film_id', 'title', 'description', 'release_year', 'language_name', 'category_name', 'rental_duration', 'rental_rate', 'film_length_minutes', 'replacement_cost', 'rating', 'special_features', 'actor_count'],
  },
  {
    name: 'dim_store',
    type: 'Dimension',
    desc: 'Chi nhánh cửa hàng (10 cột)',
    columns: ['store_id', 'manager_first_name', 'manager_last_name', 'address', 'address2', 'district', 'city_name', 'country_name', 'postal_code', 'phone'],
  },
  {
    name: 'dim_staff',
    type: 'Dimension',
    desc: 'Nhân viên phục vụ (14 cột)',
    columns: ['staff_id', 'first_name', 'last_name', 'email', 'username', 'store_id', 'is_active', 'address', 'address2', 'district', 'city_name', 'country_name', 'postal_code', 'phone'],
  },
  {
    name: 'dim_actor',
    type: 'Dimension',
    desc: 'Diễn viên & số lượng phim (4 cột)',
    columns: ['actor_id', 'first_name', 'last_name', 'film_count'],
  },
];

const typeBadgeClasses: Record<string, string> = {
  Fact: 'badge-brand',
  Dimension: 'badge-success',
  SCD2: 'badge-warning',
};

export const Sidebar: React.FC<SidebarProps> = ({ onSelectPrompt, isOpen = true }) => {
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});
  const [searchQuery, setSearchQuery] = useState('');

  const toggleTable = (name: string) => {
    setExpandedTables((prev) => ({ ...prev, [name]: !prev[name] }));
  };

  const filteredTables = GOLD_TABLES.filter(
    (t) =>
      t.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.desc.toLowerCase().includes(searchQuery.toLowerCase())
  );

  if (!isOpen) return null;

  return (
    <aside
      id="sidebar-main"
      className="w-72 h-[calc(100vh-57px)] border-r border-slate-200 bg-white flex flex-col overflow-hidden animate-slide-in-left hidden lg:flex shrink-0"
    >
      {/* Search */}
      <div className="p-3 border-b border-slate-100">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
          <input
            id="sidebar-search"
            type="text"
            placeholder="Tìm bảng, cột, SQL..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full glass-input text-xs rounded-lg pl-9 pr-3 py-2 text-slate-700 placeholder-slate-400"
          />
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {/* Quick Prompts */}
        <div className="p-3 space-y-2">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold text-slate-400 uppercase tracking-wider px-1">
            <Star className="w-3 h-3 text-brand-accent" />
            <span>Gợi ý phân tích</span>
          </div>
          <div className="space-y-1">
            {SAMPLE_PROMPTS.map((item, idx) => {
              const Icon = item.icon;
              return (
                <button
                  key={idx}
                  id={`prompt-${idx}`}
                  onClick={() => onSelectPrompt(item.prompt)}
                  className={`w-full text-left p-2.5 rounded-xl transition-all group cursor-pointer ${
                    item.highlight
                      ? 'bg-gradient-to-r from-brand-50 to-blue-50 border border-brand-200 hover:border-brand-400 shadow-sm hover:shadow-md'
                      : 'hover:bg-slate-50 border border-transparent hover:border-slate-200'
                  }`}
                >
                  <div className="flex items-start gap-2.5">
                    <div
                      className={`p-1.5 rounded-lg transition-all ${
                        item.highlight
                          ? 'bg-brand-500 text-white'
                          : 'bg-slate-100 text-slate-400 group-hover:bg-brand-500 group-hover:text-white'
                      }`}
                    >
                      <Icon className="w-3.5 h-3.5" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div
                        className={`text-[11px] font-semibold leading-tight ${
                          item.highlight ? 'text-brand-600' : 'text-slate-700 group-hover:text-brand-600'
                        }`}
                      >
                        {item.title}
                      </div>
                      <div className="text-[10px] text-slate-400 mt-0.5 truncate">
                        {item.subtitle}
                      </div>
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </div>

        {/* Schema Tree */}
        <div className="p-3 pt-1 border-t border-slate-100 space-y-2">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold text-slate-400 uppercase tracking-wider px-1">
            <Database className="w-3 h-3 text-brand-emerald" />
            <span>Semantic Data Catalog</span>
          </div>

          <div className="space-y-0.5">
            {filteredTables.map((table) => (
              <div key={table.name}>
                {/* Table Row */}
                <button
                  onClick={() => toggleTable(table.name)}
                  className="w-full flex items-center gap-2 px-2 py-1.5 rounded-lg hover:bg-slate-50 transition-all text-left cursor-pointer group"
                >
                  <span className="text-slate-400 transition-transform">
                    {expandedTables[table.name] ? (
                      <ChevronDown className="w-3 h-3" />
                    ) : (
                      <ChevronRight className="w-3 h-3" />
                    )}
                  </span>
                  <FolderOpen className="w-3.5 h-3.5 text-brand-accent flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <span className="text-[11px] font-mono font-semibold text-slate-700 group-hover:text-brand-500 transition-colors">
                      {table.name}
                    </span>
                  </div>
                  <span className={`badge ${typeBadgeClasses[table.type]} text-[8px]`}>
                    {table.type}
                  </span>
                </button>

                {/* Expanded Columns */}
                {expandedTables[table.name] && (
                  <div className="ml-7 pl-3 border-l border-slate-200 space-y-0.5 py-1 animate-fade-in">
                    <div className="text-[10px] text-slate-400 px-2 py-0.5">{table.desc}</div>
                    {table.columns.map((col) => (
                      <button
                        key={col}
                        onClick={() => onSelectPrompt(`Phân tích cột ${col} trong bảng ${table.name}`)}
                        className="w-full flex items-center gap-2 px-2 py-1 rounded-md hover:bg-brand-50 text-left cursor-pointer transition-colors"
                      >
                        <FileText className="w-3 h-3 text-slate-400" />
                        <span className="text-[10px] font-mono text-slate-500 hover:text-brand-500">
                          {col}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Footer — Upload hint */}
      <div className="p-3 border-t border-slate-100">
        <div className="flex items-center gap-2 p-3 rounded-xl border border-dashed border-slate-300 bg-slate-50/50 text-center">
          <Upload className="w-4 h-4 text-slate-400 mx-auto" />
          <div className="text-[10px] text-slate-400">
            Kéo thả file CSV, hoặc bản đồ chọn
          </div>
        </div>
      </div>
    </aside>
  );
};
