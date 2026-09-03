import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Tino Data — AI Text-to-SQL Analytics Dashboard",
  description: "Nền tảng phân tích dữ liệu thông minh với AI Agent. Hỏi bằng ngôn ngữ tự nhiên, nhận Dashboard tự động từ AWS Athena Lakehouse.",
  keywords: "AI analytics, text-to-sql, dashboard, data visualization, Tino Data",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="theme-color" content="#6366F1" />
      </head>
      <body className="antialiased bg-surface-primary text-slate-900 min-h-screen font-sans">
        {children}
      </body>
    </html>
  );
}
