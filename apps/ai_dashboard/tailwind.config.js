/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'Consolas', 'monospace'],
      },
      colors: {
        surface: {
          primary: '#F8FAFC',
          secondary: '#FFFFFF',
          tertiary: '#F1F5F9',
          hover: '#EEF2FF',
        },
        brand: {
          50: '#EEF2FF',
          100: '#E0E7FF',
          200: '#C7D2FE',
          300: '#A5B4FC',
          400: '#818CF8',
          500: '#6366F1',
          600: '#4F46E5',
          700: '#4338CA',
          accent: '#F59E0B',
          emerald: '#10B981',
          cyan: '#06B6D4',
          rose: '#F43F5E',
        },
        slate: {
          750: '#293548',
        }
      },
      boxShadow: {
        'card': '0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)',
        'card-hover': '0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -2px rgba(0,0,0,0.05)',
        'panel': '0 10px 15px -3px rgba(0,0,0,0.08), 0 4px 6px -4px rgba(0,0,0,0.05)',
        'brand': '0 4px 14px rgba(99, 102, 241, 0.25)',
        'brand-lg': '0 8px 24px rgba(99, 102, 241, 0.3)',
      },
      backgroundImage: {
        'gradient-brand': 'linear-gradient(135deg, #6366F1, #3B82F6)',
        'gradient-brand-light': 'linear-gradient(135deg, #EEF2FF, #E0E7FF)',
        'gradient-subtle': 'linear-gradient(135deg, #F8FAFC, #EEF2FF)',
        'gradient-canvas': 'linear-gradient(180deg, #F1F5F9 0%, #F8FAFC 100%)',
      },
      animation: {
        'fade-in-up': 'fadeInUp 0.45s cubic-bezier(0.16, 1, 0.3, 1) forwards',
        'slide-in-right': 'slideInRight 0.35s cubic-bezier(0.16, 1, 0.3, 1) forwards',
        'scale-in': 'scaleIn 0.3s cubic-bezier(0.16, 1, 0.3, 1) forwards',
      },
      borderRadius: {
        'xl': '14px',
        '2xl': '18px',
        '3xl': '24px',
      }
    },
  },
  plugins: [],
};
