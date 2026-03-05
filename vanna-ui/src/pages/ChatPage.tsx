import { Layout } from 'antd';
import ConversationSidebar from '../components/sidebar/ConversationSidebar';
import ChatPanel from '../components/chat/ChatPanel';
import { useConversationStore } from '../stores/conversationStore';

const { Content } = Layout;

const ChatPage: React.FC = () => {
  const activeConversationId = useConversationStore((s) => s.activeConversationId);

  return (
    <Layout style={{ height: '100vh', overflow: 'hidden' }}>
      <ConversationSidebar />
      <Content
        role="main"
        style={{
          padding: 8,
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
          gap: 4,
        }}
      >
        <ChatPanel key={activeConversationId ?? 'empty'} conversationId={activeConversationId ?? undefined} />
      </Content>
    </Layout>
  );
};

export default ChatPage;
