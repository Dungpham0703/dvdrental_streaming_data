'use client';

import React from 'react';
import {
  Sparkles,
  Database,
  Cpu,
  ShieldCheck,
  MessageCircle,
  Settings,
  ChevronDown,
  Play,
  Wand2,
  Rocket,
  Menu,
} from 'lucide-react';

interface HeaderProps {
  isChatOpen: boolean;
  onToggleChat: () => void;
  isSidebarOpen: boolean;
  onToggleSidebar: () => void;
}

export const Header: React.FC<HeaderProps> = ({ isChatOpen, onToggleChat, isSidebarOpen, onToggleSidebar }) => {
  return (
    <header
      id="header-main"
      className="glass-panel sticky top-0 z-50 px-5 py-3 border-b border-slate-200/80 flex items-center justify-between"
    >
      {/* Left — Brand & Menu */}
      <div className="flex items-center gap-3">
        <button
          onClick={onToggleSidebar}
          className={`p-2 rounded-lg transition-all cursor-pointer ${
            isSidebarOpen
              ? 'bg-slate-100 text-slate-600'
              : 'text-slate-400 hover:text-slate-600 hover:bg-slate-100'
          }`}
          title="Toggle Data Catalog"
        >
          <Menu className="w-5 h-5" />
        </button>
        <div className="w-9 h-9 rounded-xl bg-gradient-to-tr from-brand-500 to-brand-cyan p-[2px] shadow-brand">
          <div className="w-full h-full bg-white rounded-[9px] flex items-center justify-center">
            <Sparkles className="w-4.5 h-4.5 text-brand-500" />
          </div>
        </div>
        <div>
          <h1 className="text-sm font-bold text-slate-900 tracking-tight flex items-center gap-2">
            Tino Data
            <span className="badge badge-brand text-[9px]">
              TINO AI AGENT
            </span>
          </h1>
          <p className="text-[11px] text-slate-400 hidden sm:block">
            Natural Language Analytics — AWS Athena Lakehouse
          </p>
        </div>
      </div>

      {/* Center — Action Buttons */}
      <div className="hidden lg:flex items-center gap-1.5">
        <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-slate-500 hover:text-brand-500 hover:bg-brand-50 transition-all cursor-pointer">
          <Play className="w-3.5 h-3.5" />
          Lập kế hoạch
        </button>
        <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-slate-500 hover:text-brand-500 hover:bg-brand-50 transition-all cursor-pointer">
          <Wand2 className="w-3.5 h-3.5" />
          Tự động
        </button>
        <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-slate-500 hover:text-brand-500 hover:bg-brand-50 transition-all cursor-pointer">
          <Rocket className="w-3.5 h-3.5" />
          Triển khai
        </button>
      </div>

      {/* Right — Status & Chat Toggle */}
      <div className="flex items-center gap-2.5">
        {/* Status Pills */}
        <div className="hidden md:flex items-center gap-2">
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-slate-50 border border-slate-200 text-[11px] text-slate-500">
            <Database className="w-3 h-3 text-brand-emerald" />
            <span>Athena</span>
          </div>
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-slate-50 border border-slate-200 text-[11px] text-slate-500">
            <Cpu className="w-3 h-3 text-brand-cyan" />
            <span>Gemini</span>
          </div>
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-slate-50 border border-slate-200 text-[11px] text-slate-500">
            <ShieldCheck className="w-3 h-3 text-brand-emerald" />
            <span>Read-Only</span>
          </div>
        </div>

        {/* Settings */}
        <button className="p-2 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100 transition-all cursor-pointer">
          <Settings className="w-4 h-4" />
        </button>

        {/* Chat Toggle */}
        <button
          id="btn-toggle-chat"
          onClick={onToggleChat}
          className={`flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-semibold transition-all cursor-pointer ${
            isChatOpen
              ? 'bg-brand-500 text-white shadow-brand'
              : 'bg-brand-50 text-brand-500 border border-brand-200 hover:bg-brand-100'
          }`}
        >
          <MessageCircle className="w-4 h-4" />
          Hội thoại
        </button>
      </div>
    </header>
  );
};
