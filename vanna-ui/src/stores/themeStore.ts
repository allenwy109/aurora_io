import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export interface ThemeStore {
  mode: 'light' | 'dark';
  toggle: () => void;
  setMode: (mode: 'light' | 'dark') => void;
}

export const useThemeStore = create<ThemeStore>()(
  persist(
    (set) => ({
      mode: 'light',

      toggle: () =>
        set((state) => ({
          mode: state.mode === 'light' ? 'dark' : 'light',
        })),

      setMode: (mode) => set({ mode }),
    }),
    {
      name: 'vanna-theme',
    }
  )
);
