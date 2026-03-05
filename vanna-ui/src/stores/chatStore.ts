import { create } from 'zustand';
import type { Message, RichComponentData } from '../types';

export interface ChatStore {
  messages: Message[];
  isLoading: boolean;

  addMessage: (message: Message) => void;
  updateMessage: (messageId: string, updates: Partial<Message>) => void;
  appendComponent: (messageId: string, component: RichComponentData) => void;
  setLoading: (loading: boolean) => void;
  clearMessages: () => void;
  setError: (messageId: string, error: string) => void;
}

export const useChatStore = create<ChatStore>((set) => ({
  messages: [],
  isLoading: false,

  addMessage: (message) =>
    set((state) => ({ messages: [...state.messages, message] })),

  updateMessage: (messageId, updates) =>
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === messageId ? { ...msg, ...updates } : msg
      ),
    })),

  appendComponent: (messageId, component) =>
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === messageId
          ? { ...msg, components: [...msg.components, component] }
          : msg
      ),
    })),

  setLoading: (loading) => set({ isLoading: loading }),

  clearMessages: () => set({ messages: [] }),

  setError: (messageId, error) =>
    set((state) => ({
      messages: state.messages.map((msg) =>
        msg.id === messageId ? { ...msg, status: 'error', error } : msg
      ),
    })),
}));
