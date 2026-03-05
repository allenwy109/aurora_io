import { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Layout, Button, List, Typography, Popconfirm, Tooltip } from 'antd';
import {
  PlusOutlined,
  DeleteOutlined,
  SettingOutlined,
  BulbOutlined,
  BulbFilled,
  MessageOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { useConversationStore } from '../../stores/conversationStore';
import { useThemeStore } from '../../stores/themeStore';

const { Sider } = Layout;
const { Text } = Typography;

function formatTime(ts: number): string {
  const d = new Date(ts);
  const now = new Date();
  const isToday =
    d.getFullYear() === now.getFullYear() &&
    d.getMonth() === now.getMonth() &&
    d.getDate() === now.getDate();
  if (isToday) {
    return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
  }
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

export interface ConversationSidebarProps {
  collapsed?: boolean;
  onCollapsedChange?: (collapsed: boolean) => void;
}

const ConversationSidebar: React.FC<ConversationSidebarProps> = ({
  collapsed: controlledCollapsed,
  onCollapsedChange,
}) => {
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const collapsed = controlledCollapsed ?? internalCollapsed;
  const setCollapsed = (val: boolean) => {
    setInternalCollapsed(val);
    onCollapsedChange?.(val);
  };

  const navigate = useNavigate();
  const location = useLocation();

  const conversations = useConversationStore((s) => s.conversations);
  const activeConversationId = useConversationStore((s) => s.activeConversationId);
  const createConversation = useConversationStore((s) => s.createConversation);
  const deleteConversation = useConversationStore((s) => s.deleteConversation);
  const switchConversation = useConversationStore((s) => s.switchConversation);

  const themeMode = useThemeStore((s) => s.mode);
  const toggleTheme = useThemeStore((s) => s.toggle);

  const isDark = themeMode === 'dark';
  const bgColor = isDark ? '#141414' : '#fff';
  const borderColor = isDark ? '#303030' : '#f0f0f0';
  const activeItemBg = isDark ? '#1a1a2e' : '#e6f4ff';
  const textColor = isDark ? 'rgba(255,255,255,0.85)' : 'rgba(0,0,0,0.88)';
  const secondaryTextColor = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';

  const handleNewConversation = () => {
    createConversation();
    if (location.pathname !== '/') {
      navigate('/');
    }
  };

  const handleSelectConversation = (id: string) => {
    switchConversation(id);
    if (location.pathname !== '/') {
      navigate('/');
    }
  };

  const sortedConversations = [...conversations].sort(
    (a, b) => b.updatedAt - a.updatedAt
  );

  return (
    <Sider
      collapsible
      collapsed={collapsed}
      onCollapse={setCollapsed}
      trigger={null}
      width={240}
      collapsedWidth={48}
      style={{
        background: bgColor,
        borderRight: `1px solid ${borderColor}`,
        display: 'flex',
        flexDirection: 'column',
        height: '100vh',
        overflow: 'hidden',
      }}
      aria-label="Conversation sidebar"
    >
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          height: '100%',
          padding: collapsed ? '8px 4px' : '8px',
          gap: 4,
        }}
      >
        {/* Collapse toggle */}
        <div style={{ display: 'flex', justifyContent: collapsed ? 'center' : 'flex-end' }}>
          <Button
            type="text"
            size="small"
            icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
            onClick={() => setCollapsed(!collapsed)}
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            style={{ color: textColor }}
          />
        </div>

        {/* New conversation button */}
        <Tooltip title={collapsed ? '新建对话' : undefined} placement="right">
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={handleNewConversation}
            block
            size="small"
            aria-label="New conversation"
            style={{ marginBottom: 4 }}
          >
            {collapsed ? null : '新建对话'}
          </Button>
        </Tooltip>

        {/* Conversation list */}
        <div style={{ flex: 1, overflow: 'auto', minHeight: 0 }}>
          {collapsed ? (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
              {sortedConversations.map((conv) => (
                <Tooltip key={conv.id} title={conv.title} placement="right">
                  <Button
                    type="text"
                    size="small"
                    icon={<MessageOutlined />}
                    onClick={() => handleSelectConversation(conv.id)}
                    aria-label={conv.title}
                    style={{
                      background: conv.id === activeConversationId ? activeItemBg : undefined,
                      color: textColor,
                    }}
                  />
                </Tooltip>
              ))}
            </div>
          ) : (
            <List
              dataSource={sortedConversations}
              size="small"
              split={false}
              locale={{ emptyText: '暂无对话' }}
              renderItem={(conv) => (
                <List.Item
                  key={conv.id}
                  onClick={() => handleSelectConversation(conv.id)}
                  onKeyDown={(e: React.KeyboardEvent) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      e.preventDefault();
                      handleSelectConversation(conv.id);
                    }
                  }}
                  tabIndex={0}
                  role="button"
                  aria-label={`Switch to conversation: ${conv.title}`}
                  aria-current={conv.id === activeConversationId ? 'true' : undefined}
                  style={{
                    padding: '4px 8px',
                    cursor: 'pointer',
                    borderRadius: 4,
                    background: conv.id === activeConversationId ? activeItemBg : undefined,
                    marginBottom: 2,
                  }}
                  actions={[
                    <Popconfirm
                      key="delete"
                      title="确认删除此对话？"
                      onConfirm={(e) => {
                        e?.stopPropagation();
                        deleteConversation(conv.id);
                      }}
                      onCancel={(e) => e?.stopPropagation()}
                      okText="删除"
                      cancelText="取消"
                    >
                      <Button
                        type="text"
                        size="small"
                        danger
                        icon={<DeleteOutlined />}
                        onClick={(e) => e.stopPropagation()}
                        aria-label={`Delete conversation ${conv.title}`}
                        style={{ padding: '0 4px' }}
                      />
                    </Popconfirm>,
                  ]}
                >
                  <List.Item.Meta
                    title={
                      <Text
                        ellipsis
                        style={{
                          fontSize: 13,
                          color: textColor,
                          maxWidth: 140,
                        }}
                      >
                        {conv.title}
                      </Text>
                    }
                    description={
                      <Text style={{ fontSize: 11, color: secondaryTextColor }}>
                        {formatTime(conv.updatedAt)}
                      </Text>
                    }
                  />
                </List.Item>
              )}
            />
          )}
        </div>

        {/* Bottom actions: settings + theme toggle */}
        <div
          style={{
            display: 'flex',
            flexDirection: collapsed ? 'column' : 'row',
            justifyContent: collapsed ? 'center' : 'space-between',
            alignItems: 'center',
            gap: 4,
            borderTop: `1px solid ${borderColor}`,
            paddingTop: 8,
          }}
        >
          <Tooltip title={collapsed ? '设置' : undefined} placement="right">
            <Button
              type="text"
              size="small"
              icon={<SettingOutlined />}
              onClick={() => navigate('/settings')}
              aria-label="Settings"
              style={{ color: textColor }}
            >
              {collapsed ? null : '设置'}
            </Button>
          </Tooltip>
          <Tooltip title={collapsed ? (isDark ? '亮色模式' : '暗色模式') : undefined} placement="right">
            <Button
              type="text"
              size="small"
              icon={isDark ? <BulbFilled /> : <BulbOutlined />}
              onClick={toggleTheme}
              aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
              style={{ color: textColor }}
            >
              {collapsed ? null : (isDark ? '亮色' : '暗色')}
            </Button>
          </Tooltip>
        </div>
      </div>
    </Sider>
  );
};

export default ConversationSidebar;
