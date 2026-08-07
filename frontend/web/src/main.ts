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
  LoginOutlined,
  LogoutOutlined,
  MessageOutlined,
  PlusOutlined,
  RobotOutlined,
  SendOutlined,
  UserOutlined,
} from "@ant-design/icons-vue";
import "ant-design-vue/dist/reset.css";
import "./style.css";

type AuthMode = "login" | "register";
type User = { id: string; email: string; display_name: string | null };
type Conversation = { id: string; title: string; updated_at: string };
type Message = {
  id: string;
  conversation_id: string;
  role: string;
  content: string;
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
    const activeId = ref("");
    const messages = ref<Message[]>([]);
    const draft = ref("");
    const busy = ref(false);
    const booting = ref(Boolean(token.value));
    const authLoading = ref(false);
    const conversationLoading = ref(false);
    const error = ref("");
    const messagePane = ref<HTMLElement | null>(null);

    const activeConversation = computed(() =>
      conversations.value.find((item) => item.id === activeId.value),
    );
    const canSend = computed(() => Boolean(draft.value.trim() && activeId.value && !busy.value));
    const authTitle = computed(() => (mode.value === "login" ? "欢迎回来" : "创建账号"));

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
        if (!activeId.value && rows[0]) activeId.value = rows[0].id;
      } finally {
        conversationLoading.value = false;
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
        await refreshConversations();
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
        await refreshConversations();
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
        activeId.value = conversation.id;
      } catch (err) {
        setError(err);
      }
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
      messages.value = [];
      activeId.value = "";
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
                    class: ["conversation-item", item.id === activeId.value ? "active" : ""],
                    onClick: () => (activeId.value = item.id),
                  }, [
                    h("span", { class: "conversation-icon" }, [h(MessageOutlined)]),
                    h("span", { class: "conversation-title" }, item.title),
                  ]),
                ),
            }),
      ]);
    }

    function renderMessages() {
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
              h(Typography.Title, { level: 3 }, () => activeConversation.value?.title || "选择会话"),
              h(Typography.Text, { type: "secondary" }, () =>
                activeId.value ? "当前对话" : "从左侧选择一个对话",
              ),
            ]),
            busy.value ? h(Spin, { size: "small" }) : null,
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
          h("form", {
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
          ]),
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
    created_at: new Date().toISOString(),
  };
}

createApp(App).mount("#root");
