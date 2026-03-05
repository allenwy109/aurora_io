import { useState } from 'react';
import {
  Layout,
  Card,
  Form,
  Input,
  Select,
  Button,
  Radio,
  Space,
  Typography,
  message,
  Divider,
} from 'antd';
import {
  ArrowLeftOutlined,
  SaveOutlined,
  ApiOutlined,
  DatabaseOutlined,
  RobotOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useSettingsStore } from '../stores/settingsStore';
import { useThemeStore } from '../stores/themeStore';

const { Content } = Layout;
const { Title } = Typography;

const SettingsPage: React.FC = () => {
  const navigate = useNavigate();
  const [messageApi, contextHolder] = message.useMessage();

  const database = useSettingsStore((s) => s.database);
  const llm = useSettingsStore((s) => s.llm);
  const ui = useSettingsStore((s) => s.ui);
  const updateDatabase = useSettingsStore((s) => s.updateDatabase);
  const updateLLM = useSettingsStore((s) => s.updateLLM);
  const updateUI = useSettingsStore((s) => s.updateUI);
  const validateSettings = useSettingsStore((s) => s.validateSettings);
  const saveSettings = useSettingsStore((s) => s.saveSettings);
  const testConnection = useSettingsStore((s) => s.testConnection);
  const setThemeMode = useThemeStore((s) => s.setMode);

  const [errors, setErrors] = useState<Record<string, string>>({});
  const [testing, setTesting] = useState(false);

  const handleSave = async () => {
    const result = validateSettings();
    setErrors(result.errors);
    if (!result.valid) {
      messageApi.error('请修正表单中的错误');
      return;
    }
    await saveSettings();
    messageApi.success('设置已保存');
  };

  const handleTestConnection = async () => {
    setTesting(true);
    try {
      const result = await testConnection();
      if (result.success) {
        messageApi.success('连接成功');
      } else {
        messageApi.error(result.error ?? '连接失败');
      }
    } finally {
      setTesting(false);
    }
  };

  const handleThemeChange = (value: 'light' | 'dark') => {
    updateUI({ theme: value });
    setThemeMode(value);
  };

  const fieldError = (key: string) =>
    errors[key] ? { validateStatus: 'error' as const, help: errors[key] } : {};

  return (
    <Layout style={{ height: '100vh', overflow: 'auto', background: 'transparent' }}>
      {contextHolder}
      <Content style={{ padding: 16, maxWidth: 720, margin: '0 auto', width: '100%' }}>
        <Space style={{ marginBottom: 12 }}>
          <Button
            type="text"
            icon={<ArrowLeftOutlined />}
            onClick={() => navigate('/')}
            aria-label="返回聊天"
          >
            返回
          </Button>
        </Space>

        <Title level={4} style={{ marginBottom: 16 }}>
          <SettingOutlined style={{ marginRight: 8 }} />
          系统设置
        </Title>

        {/* Database Config */}
        <Card
          size="small"
          title={<><DatabaseOutlined style={{ marginRight: 6 }} />数据库连接</>}
          style={{ marginBottom: 12 }}
        >
          <Form layout="vertical" size="small">
            <Form.Item label="服务器地址" {...fieldError('database.server')}>
              <Input
                value={database.server}
                onChange={(e) => updateDatabase({ server: e.target.value })}
                placeholder="例如: myserver.database.windows.net"
              />
            </Form.Item>
            <Form.Item label="数据库名称" {...fieldError('database.database')}>
              <Input
                value={database.database}
                onChange={(e) => updateDatabase({ database: e.target.value })}
                placeholder="例如: mydb"
              />
            </Form.Item>
            <Form.Item label="认证方式">
              <Radio.Group
                value={database.authType}
                onChange={(e) => updateDatabase({ authType: e.target.value })}
              >
                <Radio value="windows">Windows 认证</Radio>
                <Radio value="sql">SQL Server 认证</Radio>
              </Radio.Group>
            </Form.Item>
            {database.authType === 'sql' && (
              <>
                <Form.Item label="用户名" {...fieldError('database.username')}>
                  <Input
                    value={database.username ?? ''}
                    onChange={(e) => updateDatabase({ username: e.target.value })}
                    placeholder="数据库用户名"
                  />
                </Form.Item>
                <Form.Item label="密码" {...fieldError('database.password')}>
                  <Input.Password
                    value={database.password ?? ''}
                    onChange={(e) => updateDatabase({ password: e.target.value })}
                    placeholder="数据库密码"
                  />
                </Form.Item>
              </>
            )}
            <Form.Item label="驱动程序" {...fieldError('database.driver')}>
              <Input
                value={database.driver}
                onChange={(e) => updateDatabase({ driver: e.target.value })}
                placeholder="ODBC Driver 17 for SQL Server"
              />
            </Form.Item>
            <Form.Item>
              <Button
                icon={<ApiOutlined />}
                onClick={handleTestConnection}
                loading={testing}
                aria-label="Test database connection"
              >
                测试连接
              </Button>
            </Form.Item>
          </Form>
        </Card>

        {/* LLM Config */}
        <Card
          size="small"
          title={<><RobotOutlined style={{ marginRight: 6 }} />LLM 服务配置</>}
          style={{ marginBottom: 12 }}
        >
          <Form layout="vertical" size="small">
            <Form.Item label="模型名称" {...fieldError('llm.model')}>
              <Input
                value={llm.model}
                onChange={(e) => updateLLM({ model: e.target.value })}
                placeholder="例如: MiniMax-Text-01"
              />
            </Form.Item>
            <Form.Item label="API Key" {...fieldError('llm.apiKey')}>
              <Input.Password
                value={llm.apiKey}
                onChange={(e) => updateLLM({ apiKey: e.target.value })}
                placeholder="API Key"
              />
            </Form.Item>
            <Form.Item label="Base URL" {...fieldError('llm.baseUrl')}>
              <Input
                value={llm.baseUrl}
                onChange={(e) => updateLLM({ baseUrl: e.target.value })}
                placeholder="https://api.example.com/v1"
              />
            </Form.Item>
          </Form>
        </Card>

        {/* UI Preferences */}
        <Card
          size="small"
          title={<><SettingOutlined style={{ marginRight: 6 }} />界面偏好</>}
          style={{ marginBottom: 12 }}
        >
          <Form layout="vertical" size="small">
            <Form.Item label="主题">
              <Radio.Group
                value={ui.theme}
                onChange={(e) => handleThemeChange(e.target.value)}
              >
                <Radio value="light">亮色</Radio>
                <Radio value="dark">暗色</Radio>
              </Radio.Group>
            </Form.Item>
            <Form.Item label="语言">
              <Select
                value={ui.language}
                onChange={(value) => updateUI({ language: value })}
                options={[
                  { label: '中文', value: 'zh-CN' },
                  { label: 'English', value: 'en-US' },
                ]}
                style={{ width: 160 }}
              />
            </Form.Item>
          </Form>
        </Card>

        <Divider style={{ margin: '12px 0' }} />

        <Button type="primary" icon={<SaveOutlined />} onClick={handleSave} aria-label="Save settings">
          保存设置
        </Button>
      </Content>
    </Layout>
  );
};

export default SettingsPage;
