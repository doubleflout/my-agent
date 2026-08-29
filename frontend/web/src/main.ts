import {
  computed,
  createApp,
  defineComponent,
  h,
  nextTick,
  onMounted,
  ref,
  watch,
} from "vue";
import {
  Alert,
  Avatar,
  Button,
  ConfigProvider,
  Divider,
  Empty,
  Input,
  Layout,
  List,
  Space,
  Spin,
  Typography,
  theme,
} from "ant-design-vue";
import {
  ApiOutlined,
  CheckCircleOutlined,
  ClockCircleOutlined,
  LoginOutlined,
  LogoutOutlined,
  MessageOutlined,
  PlusOutlined,
  RobotOutlined,
  SendOutlined,
  StopOutlined,
  UserOutlined,
} from "@ant-design/icons-vue";
import "ant-design-vue/dist/reset.css";
import "./style.css";

type AuthMode = "login" | "register";
type MainView = "chat" | "sources" | "schedules";
type User = { id: string; email: string; display_name: string | null };
type Conversation = { id: string; title: string; session_key?: string | null; updated_at: string };
type MessageSource = {
  id: string;
  name: string;
  type: string;
  enabled: boolean;
  server?: string | null;
  get_tool?: string | null;
  ack_tool?: string | null;
  description?: string | null;
};
type ScheduleItem = {
  id: string;
  name: string;
  trigger: string;
  tier: string;
  enabled: boolean;
  fire_at?: string | null;
  timezone?: string | null;
  channel?: string | null;
  chat_id?: string | null;
  session_key?: string | null;
  run_count: number;
  action_preview: string;
};
type Message = {
  id: string;
  conversation_id: string;
  role: string;
  content: string;
  thinking?: string;
  streaming?: boolean;
  created_at: string;
};

const TOKEN_KEY = "akashic_web_token";

const App = defineComponent({
  name: "AkashicWebApp",
  setup() {
    const token = ref(localStorage.getItem(TOKEN_KEY) || "");
    const user = ref<User | null>(null);
    const mode = ref<AuthMode>("login");
    const email = ref("");
    const password = ref("");
    const displayName = ref("");
    const conversations = ref<Conversation[]>([]);
    const proactiveConversation = ref<Conversation | null>(null);
    const sources = ref<MessageSource[]>([]);
    const scheduledJobs = ref<ScheduleItem[]>([]);
    const activeId = ref("");
    const mainView = ref<MainView>("chat");
    const messages = ref<Message[]>([]);
    const draft = ref("");
    const busy = ref(false);
    const booting = ref(Boolean(token.value));
    const authLoading = ref(false);
    const conversationLoading = ref(false);
    const sourcesLoading = ref(false);
    const schedulesLoading = ref(false);
    const error = ref("");
    const messagePane = ref<HTMLElement | null>(null);

    const activeConversation = computed(() =>
      proactiveConversation.value?.id === activeId.value
        ? proactiveConversation.value
        : conversations.value.find((item) => item.id === activeId.value),
    );
    const canSend = computed(() => Boolean(draft.value.trim() && activeId.value && !busy.value));
    const authTitle = computed(() => (mode.value === "login" ? "欢迎回来" : "创建账号"));
    const enabledSourceCount = computed(() => sources.value.filter((item) => item.enabled).length);
    const enabledScheduleCount = computed(() => scheduledJobs.value.filter((item) => item.enabled).length);

    function authedHeaders(extra?: HeadersInit) {
      const headers = new Headers(extra);
      if (token.value) headers.set("Authorization", `Bearer ${token.value}`);
      return headers;
    }

    async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
      const headers = authedHeaders(init.headers);
      if (init.body) headers.set("Content-Type", "application/json");
      const response = await fetch(path, { ...init, headers });
      if (!response.ok) throw new Error(await response.text());
      return response.json();
    }

    function setError(err: unknown) {
      error.value = err instanceof Error ? err.message : String(err);
    }

    async function loadMe() {
      user.value = await request<User>("/api/auth/me");
    }

    async function refreshConversations() {
      conversationLoading.value = true;
      try {
        const rows = await request<Conversation[]>("/api/conversations");
        conversations.value = rows;
        if (!activeId.value && proactiveConversation.value) {
          activeId.value = proactiveConversation.value.id;
        } else if (!activeId.value && rows[0]) {
          activeId.value = rows[0].id;
        }
      } finally {
        conversationLoading.value = false;
      }
    }

    async function refreshProactiveConversation() {
      proactiveConversation.value = await request<Conversation>("/api/proactive/conversation");
      if (!activeId.value) activeId.value = proactiveConversation.value.id;
    }

    async function refreshSources() {
      sourcesLoading.value = true;
      try {
        sources.value = await request<MessageSource[]>("/api/proactive/sources");
      } finally {
        sourcesLoading.value = false;
      }
    }

    async function refreshSchedules() {
      schedulesLoading.value = true;
      try {
        scheduledJobs.value = await request<ScheduleItem[]>("/api/schedules");
      } finally {
        schedulesLoading.value = false;
      }
    }

    async function loadMessages(conversationId: string) {
      messages.value = await request<Message[]>(`/api/conversations/${conversationId}/messages`);
      await scrollToBottom();
    }

    async function bootstrap() {
      if (!token.value) {
        booting.value = false;
        return;
      }
      try {
        await loadMe();
        await refreshProactiveConversation();
        await refreshConversations();
        await refreshSources();
        await refreshSchedules();
      } catch {
        localStorage.removeItem(TOKEN_KEY);
        token.value = "";
        user.value = null;
      } finally {
        booting.value = false;
      }
    }

    async function submitAuth() {
      error.value = "";
      authLoading.value = true;
      try {
        const payload = {
          email: email.value.trim(),
          password: password.value,
          display_name: displayName.value.trim() || undefined,
        };
        const res = await request<{ access_token: string }>(
          mode.value === "login" ? "/api/auth/login" : "/api/auth/register",
          { method: "POST", body: JSON.stringify(payload) },
        );
        token.value = res.access_token;
        localStorage.setItem(TOKEN_KEY, res.access_token);
        await loadMe();
        await refreshProactiveConversation();
        await refreshConversations();
        await refreshSources();
        await refreshSchedules();
      } catch (err) {
        setError(err);
      } finally {
        authLoading.value = false;
      }
    }

    async function createConversation() {
      error.value = "";
      try {
        const title = `新会话 ${conversations.value.length + 1}`;
        const conversation = await request<Conversation>("/api/conversations", {
          method: "POST",
          body: JSON.stringify({ title }),
        });
        conversations.value = [conversation, ...conversations.value];
        selectConversation(conversation.id);
      } catch (err) {
        setError(err);
      }
    }

    function selectConversation(id: string) {
      mainView.value = "chat";
      activeId.value = id;
    }

    async function showSources() {
      mainView.value = "sources";
      try {
        await refreshSources();
      } catch (err) {
        setError(err);
      }
    }

    async function showSchedules() {
      mainView.value = "schedules";
      try {
        await refreshSchedules();
      } catch (err) {
        setError(err);
      }
    }

    function formatScheduleTime(value?: string | null) {
      if (!value) return "未设置";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return new Intl.DateTimeFormat("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      }).format(date);
    }

    function scheduleKindText(item: ScheduleItem) {
      const triggerMap: Record<string, string> = {
        at: "定点",
        after: "延迟",
        every: "循环",
      };
      const tierMap: Record<string, string> = {
        instant: "固定推送",
        soft: "AI 生成",
      };
      return `${triggerMap[item.trigger] || item.trigger || "任务"} / ${tierMap[item.tier] || item.tier || "执行"}`;
    }

    async function sendMessage() {
      const content = draft.value.trim();
      if (!content || !activeId.value || busy.value) return;
      error.value = "";
      draft.value = "";
      busy.value = true;
      try {
        const created = await request<{ message: Message; turn_id: string }>(
          `/api/conversations/${activeId.value}/messages`,
          { method: "POST", body: JSON.stringify({ content }) },
        );
        messages.value = [
          ...messages.value,
          created.message,
          pendingAssistant(created.turn_id, created.message.conversation_id),
        ];
        await scrollToBottom();
        await streamTurn(created.turn_id);
        await refreshConversations();
      } catch (err) {
        setError(err);
      } finally {
        busy.value = false;
      }
    }

    async function streamTurn(turnId: string) {
      const response = await fetch(`/api/turns/${turnId}/stream`, {
        headers: authedHeaders(),
      });
      if (!response.ok || !response.body) throw new Error(await response.text());
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const parts = buffer.split("\n\n");
        buffer = parts.pop() || "";
        for (const part of parts) applySseBlock(turnId, part);
      }
    }

    function applySseBlock(turnId: string, block: string) {
      const event = block.match(/^event: (.+)$/m)?.[1];
      const raw = block.match(/^data: (.+)$/m)?.[1] || "{}";
      const data = JSON.parse(raw);
      if (event === "content_delta") {
        messages.value = messages.value.map((item) =>
          item.id === `pending:${turnId}`
            ? { ...item, content: item.content + String(data.text || "") }
            : item,
        );
        void scrollToBottom();
      }
      if (event === "thinking_delta") {
        messages.value = messages.value.map((item) =>
          item.id === `pending:${turnId}`
            ? { ...item, thinking: String(item.thinking || "") + String(data.text || "") }
            : item,
        );
        void scrollToBottom();
      }
      if (event === "done") {
        messages.value = messages.value.map((item) =>
          item.id === `pending:${turnId}` ? { ...item, streaming: false } : item,
        );
      }
      if (event === "error") error.value = String(data.message || "Agent failed");
    }

    async function scrollToBottom() {
      await nextTick();
      if (messagePane.value) messagePane.value.scrollTop = messagePane.value.scrollHeight;
    }

    function logout() {
      localStorage.removeItem(TOKEN_KEY);
      token.value = "";
      user.value = null;
      conversations.value = [];
      proactiveConversation.value = null;
      sources.value = [];
      scheduledJobs.value = [];
      messages.value = [];
      activeId.value = "";
      mainView.value = "chat";
      password.value = "";
    }

    watch(activeId, (id) => {
      if (id) void loadMessages(id).catch(setError);
    });

    onMounted(() => {
      void bootstrap();
    });

    function renderAuth() {
      return h("main", { class: "auth-page" }, [
        h("section", { class: "auth-card" }, [
          h(Space, { direction: "vertical", size: 18, class: "full" }, () => [
            h("div", { class: "auth-card-head" }, [
              h(Typography.Title, { level: 3 }, () => authTitle.value),
              h(Typography.Text, { type: "secondary" }, () =>
                mode.value === "login" ? "登录后继续你的聊天" : "注册一个新的 Web 用户",
              ),
            ]),
            h("div", { class: "auth-switch" }, [
              h(Button, {
                type: mode.value === "login" ? "primary" : "default",
                onClick: () => (mode.value = "login"),
              }, () => "登录"),
              h(Button, {
                type: mode.value === "register" ? "primary" : "default",
                onClick: () => (mode.value = "register"),
              }, () => "注册"),
            ]),
            h("form", {
              class: "auth-form",
              onSubmit: (event: Event) => {
                event.preventDefault();
                void submitAuth();
              },
            }, [
              h(Input, {
                value: email.value,
                size: "large",
                placeholder: "邮箱",
                onInput: (event: Event) => (email.value = (event.target as HTMLInputElement).value),
              }),
              mode.value === "register"
                ? h(Input, {
                    value: displayName.value,
                    size: "large",
                    placeholder: "显示名称",
                    onInput: (event: Event) =>
                      (displayName.value = (event.target as HTMLInputElement).value),
                  })
                : null,
              h(Input.Password, {
                value: password.value,
                size: "large",
                placeholder: "密码",
                onInput: (event: Event) =>
                  (password.value = (event.target as HTMLInputElement).value),
              }),
              error.value ? h(Alert, { type: "error", message: error.value, showIcon: true }) : null,
              h(Button, {
                type: "primary",
                size: "large",
                block: true,
                htmlType: "submit",
                loading: authLoading.value,
              }, () => [
                h(LoginOutlined),
                mode.value === "login" ? "登录" : "创建账号",
              ]),
            ]),
          ]),
        ]),
      ]);
    }

    function renderConversationList() {
      return h("aside", { class: "sidebar" }, [
        h("div", { class: "sidebar-head" }, [
          h("div", { class: "product" }, [
            h("span", { class: "brand-mark small" }, "A"),
            h("div", [h("strong", "Akashic"), h("span", user.value?.email || "Web Chat")]),
          ]),
          h(Button, {
            type: "text",
            title: "退出登录",
            onClick: logout,
          }, () => h(LogoutOutlined)),
        ]),
        h(Button, {
          type: "primary",
          block: true,
          onClick: () => void createConversation(),
        }, () => [h(PlusOutlined), "新建会话"]),
        h("div", { class: "sidebar-label" }, "所有对话"),
        h(Divider),
        proactiveConversation.value
          ? h("button", {
              class: [
                "conversation-item",
                "proactive-entry",
                mainView.value === "chat" && proactiveConversation.value.id === activeId.value ? "active" : "",
              ],
              onClick: () => selectConversation(proactiveConversation.value?.id || ""),
            }, [
              h("span", { class: "conversation-icon" }, [h(RobotOutlined)]),
              h("span", { class: "conversation-title" }, proactiveConversation.value.title || "主动推送"),
            ])
          : null,
        conversationLoading.value
          ? h("div", { class: "sidebar-loading" }, [h(Spin), h("span", "加载会话")])
          : h(List, {
              dataSource: conversations.value,
              class: "conversation-list",
              split: false,
            }, {
              renderItem: ({ item }: { item: Conversation }) =>
                h(List.Item, null, () =>
                  h("button", {
                    class: [
                      "conversation-item",
                      mainView.value === "chat" && item.id === activeId.value ? "active" : "",
                    ],
                    onClick: () => selectConversation(item.id),
                  }, [
                    h("span", { class: "conversation-icon" }, [h(MessageOutlined)]),
                    h("span", { class: "conversation-title" }, item.title),
                  ]),
                ),
            }),
        h("div", { class: "sidebar-bottom" }, [
          h("button", {
            class: ["source-entry", mainView.value === "sources" ? "active" : ""],
            onClick: () => void showSources(),
          }, [
            h("span", { class: "conversation-icon" }, [h(ApiOutlined)]),
            h("span", { class: "conversation-title" }, "消息源"),
            h("span", { class: "source-count" }, String(enabledSourceCount.value)),
          ]),
          h("button", {
            class: ["source-entry", mainView.value === "schedules" ? "active" : ""],
            onClick: () => void showSchedules(),
          }, [
            h("span", { class: "conversation-icon" }, [h(ClockCircleOutlined)]),
            h("span", { class: "conversation-title" }, "定时任务"),
            h("span", { class: "source-count" }, String(enabledScheduleCount.value)),
          ]),
        ]),
      ]);
    }

    function renderSources() {
      return h("div", { class: "sources-view" }, [
        sourcesLoading.value
          ? h("div", { class: "empty-chat" }, [h(Spin), h("span", "加载消息源")])
          : sources.value.length === 0
            ? h("div", { class: "empty-chat" }, [h(Empty, { description: "还没有订阅消息源" })])
            : h("div", { class: "source-list-panel" }, sources.value.map((source) =>
                h("article", { key: source.id, class: "source-row" }, [
                  h("span", { class: ["source-state", source.enabled ? "enabled" : "disabled"] }, [
                    h(source.enabled ? CheckCircleOutlined : StopOutlined),
                  ]),
                  h("div", { class: "source-main" }, [
                    h("div", { class: "source-title-line" }, [
                      h("strong", source.name || source.id),
                      h("span", { class: "source-pill" }, source.type || "content"),
                    ]),
                    source.description
                      ? h("p", { class: "source-description" }, source.description)
                      : null,
                    h("div", { class: "source-meta" }, [
                      source.server ? h("span", `server: ${source.server}`) : null,
                      source.get_tool ? h("span", `get: ${source.get_tool}`) : null,
                      source.ack_tool ? h("span", `ack: ${source.ack_tool}`) : null,
                    ].filter(Boolean)),
                  ]),
                ]),
              )),
      ]);
    }

    function renderSchedules() {
      return h("div", { class: "sources-view" }, [
        schedulesLoading.value
          ? h("div", { class: "empty-chat" }, [h(Spin), h("span", "加载定时任务")])
          : scheduledJobs.value.length === 0
            ? h("div", { class: "empty-chat" }, [h(Empty, { description: "还没有定时任务" })])
            : h("div", { class: "source-list-panel" }, scheduledJobs.value.map((job) =>
                h("article", { key: job.id, class: "source-row" }, [
                  h("span", { class: ["source-state", job.enabled ? "enabled" : "disabled"] }, [
                    h(job.enabled ? CheckCircleOutlined : StopOutlined),
                  ]),
                  h("div", { class: "source-main" }, [
                    h("div", { class: "source-title-line" }, [
                      h("strong", job.name || job.id),
                      h("span", { class: "source-pill" }, scheduleKindText(job)),
                    ]),
                    job.action_preview
                      ? h("p", { class: "source-description" }, job.action_preview)
                      : null,
                    h("div", { class: "source-meta" }, [
                      h("span", `下次: ${formatScheduleTime(job.fire_at)}`),
                      job.timezone ? h("span", `tz: ${job.timezone}`) : null,
                      job.channel ? h("span", `channel: ${job.channel}`) : null,
                      job.chat_id ? h("span", `chat: ${job.chat_id}`) : null,
                      h("span", `运行: ${job.run_count} 次`),
                    ].filter(Boolean)),
                  ]),
                ]),
              )),
      ]);
    }

    function renderMessages() {
      if (mainView.value === "sources") return renderSources();
      if (mainView.value === "schedules") return renderSchedules();
      if (!activeId.value) {
        return h("div", { class: "empty-chat" }, [
          h(Empty, { description: "创建或选择一个会话开始聊天" }),
        ]);
      }
      return h("div", { class: "message-scroll", ref: messagePane }, [
        messages.value.length === 0
          ? h("div", { class: "empty-chat" }, [h(Empty, { description: "还没有消息" })])
          : messages.value.map((message) => {
              const assistant = message.role === "assistant";
              return h("article", {
                key: message.id,
                class: ["message-row", assistant ? "assistant" : "user"],
              }, [
                h(Avatar, {
                  class: "message-avatar",
                  icon: h(assistant ? RobotOutlined : UserOutlined),
                }),
                h("div", { class: "bubble" }, [
                  h("div", { class: "bubble-name" }, assistant ? "Akashic" : "You"),
                  assistant && message.thinking
                    ? h("div", { class: "thinking-box" }, [
                        h("div", { class: "thinking-title" }, "思考"),
                        h("div", { class: "thinking-content" }, message.thinking),
                      ])
                    : null,
                  h("div", { class: "bubble-content" },
                    message.content || (busy.value && assistant ? "正在思考..." : ""),
                  ),
                ]),
              ]);
            }),
      ]);
    }

    function renderChat() {
      return h(Layout, { class: "chat-layout" }, () => [
        renderConversationList(),
        h(Layout.Content, { class: "chat-main" }, () => [
          h("header", { class: "chat-header" }, [
            h("div", [
              h(Typography.Title, { level: 3 }, () =>
                mainView.value === "sources"
                  ? "消息源"
                  : mainView.value === "schedules"
                    ? "定时任务"
                    : activeConversation.value?.title || "选择会话",
              ),
              h(Typography.Text, { type: "secondary" }, () =>
                mainView.value === "sources"
                  ? `已启用 ${enabledSourceCount.value} 个订阅`
                  : mainView.value === "schedules"
                    ? `已启用 ${enabledScheduleCount.value} 个任务`
                  : activeId.value ? "当前对话" : "从左侧选择一个对话",
              ),
            ]),
            busy.value || sourcesLoading.value || schedulesLoading.value ? h(Spin, { size: "small" }) : null,
          ]),
          renderMessages(),
          error.value ? h(Alert, {
            class: "chat-error",
            type: "error",
            message: error.value,
            showIcon: true,
            closable: true,
            onClose: () => (error.value = ""),
          }) : null,
          mainView.value === "chat"
            ? h("form", {
                class: "composer",
                onSubmit: (event: Event) => {
                  event.preventDefault();
                  void sendMessage();
                },
              }, [
                h(Input.TextArea, {
                  value: draft.value,
                  rows: 2,
                  placeholder: activeId.value ? "输入消息，和 Agent 继续推进任务..." : "请先创建会话",
                  disabled: !activeId.value || busy.value,
                  onInput: (event: Event) => (draft.value = (event.target as HTMLTextAreaElement).value),
                  onKeydown: (event: KeyboardEvent) => {
                    if (event.key === "Enter" && !event.shiftKey) {
                      event.preventDefault();
                      void sendMessage();
                    }
                  },
                }),
                h(Button, {
                  type: "primary",
                  size: "large",
                  disabled: !canSend.value,
                  loading: busy.value,
                  htmlType: "submit",
                }, () => [h(SendOutlined), "发送"]),
              ])
            : null,
        ]),
      ]);
    }

    return () =>
      h(ConfigProvider, {
        theme: {
          algorithm: theme.defaultAlgorithm,
          token: {
            colorPrimary: "#2563eb",
            borderRadius: 8,
            fontFamily:
              "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
          },
        },
      }, () =>
        booting.value
          ? h("main", { class: "boot-screen" }, [h(Spin, { size: "large" }), h("span", "加载中")])
          : token.value && user.value
            ? renderChat()
            : renderAuth(),
      );
  },
});

function pendingAssistant(turnId: string, conversationId: string): Message {
  return {
    id: `pending:${turnId}`,
    conversation_id: conversationId,
    role: "assistant",
    content: "",
    thinking: "",
    streaming: true,
    created_at: new Date().toISOString(),
  };
}

createApp(App).mount("#root");
