import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { DatabaseConfig, LLMConfig, UIPreferences } from '../types';

export interface SettingsStore {
  database: DatabaseConfig;
  llm: LLMConfig;
  ui: UIPreferences;

  updateDatabase: (config: Partial<DatabaseConfig>) => void;
  updateLLM: (config: Partial<LLMConfig>) => void;
  updateUI: (prefs: Partial<UIPreferences>) => void;
  testConnection: () => Promise<{ success: boolean; error?: string }>;
  saveSettings: () => Promise<void>;
  validateSettings: () => { valid: boolean; errors: Record<string, string> };
}

export const useSettingsStore = create<SettingsStore>()(
  persist(
    (set, get) => ({
      database: {
        server: '',
        database: '',
        authType: 'windows' as const,
        driver: 'ODBC Driver 17 for SQL Server',
      },
      llm: {
        model: '',
        apiKey: '',
        baseUrl: '',
      },
      ui: {
        theme: 'light' as const,
        language: 'zh-CN' as const,
      },

      updateDatabase: (config) => {
        set((state) => ({
          database: { ...state.database, ...config },
        }));
      },

      updateLLM: (config) => {
        set((state) => ({
          llm: { ...state.llm, ...config },
        }));
      },

      updateUI: (prefs) => {
        set((state) => ({
          ui: { ...state.ui, ...prefs },
        }));
      },

      testConnection: async () => {
        return { success: false, error: 'Not implemented' };
      },

      saveSettings: async () => {
        // Zustand persist middleware handles localStorage automatically.
        // This method triggers a re-persist by touching state.
        const { database, llm, ui } = get();
        set({ database, llm, ui });
      },

      validateSettings: () => {
        const { database, llm } = get();
        const errors: Record<string, string> = {};

        if (!database.server.trim()) {
          errors['database.server'] = '服务器地址不能为空';
        }
        if (!database.database.trim()) {
          errors['database.database'] = '数据库名称不能为空';
        }
        if (!database.driver.trim()) {
          errors['database.driver'] = '驱动程序不能为空';
        }
        if (database.authType === 'sql') {
          if (!database.username?.trim()) {
            errors['database.username'] = 'SQL 认证模式下用户名不能为空';
          }
          if (!database.password?.trim()) {
            errors['database.password'] = 'SQL 认证模式下密码不能为空';
          }
        }

        if (!llm.model.trim()) {
          errors['llm.model'] = '模型名称不能为空';
        }
        if (!llm.apiKey.trim()) {
          errors['llm.apiKey'] = 'API Key 不能为空';
        }
        if (!llm.baseUrl.trim()) {
          errors['llm.baseUrl'] = 'Base URL 不能为空';
        } else if (
          !llm.baseUrl.startsWith('http://') &&
          !llm.baseUrl.startsWith('https://')
        ) {
          errors['llm.baseUrl'] = 'Base URL 必须以 http:// 或 https:// 开头';
        }

        return {
          valid: Object.keys(errors).length === 0,
          errors,
        };
      },
    }),
    {
      name: 'vanna-settings',
    }
  )
);
