'use client';

import React, { useState, useCallback } from 'react';
import { Header } from '@/components/Header';
import { Sidebar } from '@/components/Sidebar';
import { DashboardCanvas } from '@/components/DashboardCanvas';
import { ChatInterface } from '@/components/ChatInterface';

interface WidgetData {
  title: string;
  sql: string;
  chartType: string;
  columns: string[];
  data: any[];
  execution_time_ms?: number;
  mode?: string;
}

export default function Home() {
  const [isChatOpen, setIsChatOpen] = useState(true);
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [selectedPrompt, setSelectedPrompt] = useState<string | undefined>(undefined);
  const [dashboardWidgets, setDashboardWidgets] = useState<WidgetData[]>([]);
  const [dashboardTitle, setDashboardTitle] = useState<string | undefined>();
  const [dashboardSummary, setDashboardSummary] = useState<string | undefined>();

  const handleSelectPrompt = (prompt: string) => {
    if (!isChatOpen) setIsChatOpen(true);
    setSelectedPrompt(prompt);
  };

  const handleClearPrompt = () => {
    setSelectedPrompt(undefined);
  };

  const handleToggleChat = () => {
    setIsChatOpen((prev) => !prev);
  };

  const handleToggleSidebar = () => {
    setIsSidebarOpen((prev) => !prev);
  };

  const handleDashboardReady = useCallback(
    (widgets: WidgetData[], title?: string, summary?: string) => {
      setDashboardWidgets(widgets);
      setDashboardTitle(title);
      setDashboardSummary(summary);
    },
    []
  );

  return (
    <div className="min-h-screen bg-surface-primary flex flex-col font-sans">
      {/* Top Navigation Header */}
      <Header
        isChatOpen={isChatOpen}
        onToggleChat={handleToggleChat}
        isSidebarOpen={isSidebarOpen}
        onToggleSidebar={handleToggleSidebar}
      />

      {/* Main 3-Column Layout */}
      <div className="flex-1 flex overflow-hidden relative">
        {/* Left Sidebar — Data Catalog & Quick Prompts */}
        <Sidebar
          onSelectPrompt={handleSelectPrompt}
          isOpen={isSidebarOpen}
        />

        {/* Center — Dashboard Canvas */}
        <DashboardCanvas
          dashboardTitle={dashboardTitle}
          dashboardSummary={dashboardSummary}
          widgets={dashboardWidgets}
          onSelectPrompt={handleSelectPrompt}
        />

        {/* Right — AI Chat Panel */}
        <ChatInterface
          isOpen={isChatOpen}
          onClose={() => setIsChatOpen(false)}
          onDashboardReady={handleDashboardReady}
          selectedPrompt={selectedPrompt}
          onClearSelectedPrompt={handleClearPrompt}
        />
      </div>
    </div>
  );
}
