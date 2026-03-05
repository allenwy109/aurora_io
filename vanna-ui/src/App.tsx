import { useEffect, useMemo } from 'react';
import { Routes, Route } from 'react-router-dom';
import { ConfigProvider, theme } from 'antd';
import ChatPage from './pages/ChatPage';
import SettingsPage from './pages/SettingsPage';
import { useThemeStore } from './stores/themeStore';

const App = () => {
  const mode = useThemeStore((s) => s.mode);
  const isDark = mode === 'dark';

  const algorithm = useMemo(
    () =>
      isDark
        ? [theme.compactAlgorithm, theme.darkAlgorithm]
        : [theme.compactAlgorithm],
    [isDark]
  );

  // Sync body background with theme mode
  useEffect(() => {
    document.body.style.backgroundColor = isDark ? '#141414' : '#ffffff';
    document.body.style.color = isDark
      ? 'rgba(255, 255, 255, 0.85)'
      : 'rgba(0, 0, 0, 0.88)';
  }, [isDark]);

  return (
    <ConfigProvider
      theme={{
        algorithm,
        token: {
          padding: 8,
          margin: 4,
        },
      }}
    >
      <Routes>
        <Route path="/" element={<ChatPage />} />
        <Route path="/settings" element={<SettingsPage />} />
      </Routes>
    </ConfigProvider>
  );
};

export default App;
