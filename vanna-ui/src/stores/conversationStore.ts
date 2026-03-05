import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { Conversation, Message } from '../types';

export interface ConversationStore {
  conversations: Conversation[];
  activeConversationId: string | null;

  createConversation: () => string;
  deleteConversation: (id: string) => void;
  switchConversation: (id: string) => void;
  addMessage: (conversationId: string, message: Message) => void;
  updateMessage: (conversationId: string, messageId: string, updates: Partial<Message>) => void;
  generateTitle: (conversationId: string) => void;
}

let idCounter = 0;

function generateId(): string {
  return `conv-${Date.now()}-${++idCounter}`;
}

export const useConversationStore = create<ConversationStore>()(
  persist(
    (set, get) => ({
      conversations: [],
      activeConversationId: null,

      createConversation: () => {
        const id = generateId();
        const now = Date.now();
        const conversation: Conversation = {
          id,
          title: '新对话',
          messages: [],
          createdAt: now,
          updatedAt: now,
        };
        set((state) => ({
          conversations: [...state.conversations, conversation],
          activeConversationId: id,
        }));
        return id;
      },

      deleteConversation: (id) => {
        set((state) => {
          const remaining = state.conversations.filter((c) => c.id !== id);
          let nextActiveId = state.activeConversationId;
          if (state.activeConversationId === id) {
            nextActiveId = remaining.length > 0 ? remaining[0].id : null;
          }
          return {
            conversations: remaining,
            activeConversationId: nextActiveId,
          };
        });
      },

      switchConversation: (id) => {
        set({ activeConversationId: id });
      },

      addMessage: (conversationId, message) => {
        set((state) => ({
          conversations: state.conversations.map((c) =>
            c.id === conversationId
              ? { ...c, messages: [...c.messages, message], updatedAt: Date.now() }
              : c
          ),
        }));
      },

      updateMessage: (conversationId, messageId, updates) => {
        set((state) => ({
          conversations: state.conversations.map((c) =>
            c.id === conversationId
              ? {
                  ...c,
                  messages: c.messages.map((m) =>
                    m.id === messageId ? { ...m, ...updates } : m
                  ),
                  updatedAt: Date.now(),
                }
              : c
          ),
        }));
      },

      generateTitle: (conversationId) => {
        const state = get();
        const conversation = state.conversations.find((c) => c.id === conversationId);
        if (!conversation) return;

        const firstUserMessage = conversation.messages.find((m) => m.role === 'user');
        if (!firstUserMessage || !firstUserMessage.content) return;

        const content = firstUserMessage.content.trim();
        const title = content.length > 30 ? content.slice(0, 30) + '...' : content;

        set((state) => ({
          conversations: state.conversations.map((c) =>
            c.id === conversationId ? { ...c, title } : c
          ),
        }));
      },
    }),
    {
      name: 'vanna-conversations',
    }
  )
);
