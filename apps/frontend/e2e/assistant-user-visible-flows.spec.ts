import { expect, Page, test } from "@playwright/test";

const USER_EMAIL = process.env.E2E_USER_EMAIL || "user@example.com";
const USER_PASSWORD = process.env.E2E_USER_PASSWORD || "ChangeMe123!";
const OPERATOR_EMAIL = process.env.E2E_OPERATOR_EMAIL || "operator@example.com";
const OPERATOR_PASSWORD = process.env.E2E_OPERATOR_PASSWORD || "ChangeMe123!";

function safeId(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function uniqueMessage(prefix: string): string {
  return `${prefix} [e2e-${Date.now()}-${Math.random().toString(16).slice(2, 8)}]`;
}

async function login(page: Page, email: string, password: string): Promise<void> {
  await page.goto("/login");
  await expect(page.getByTestId("login-form")).toBeVisible();
  await page.getByTestId("login-email").fill(email);
  await page.getByTestId("login-password").fill(password);
  await page.getByTestId("login-remember-mode").selectOption("localStorage");
  await page.getByTestId("login-submit").click();
  await page.waitForURL(/\/runs|\/assistant/, { timeout: 60_000 });
  await page.goto("/assistant");
  await expect(page.getByTestId("assistant-page")).toBeVisible();
}

async function resetToFreshConversation(page: Page): Promise<void> {
  await page.getByTestId("assistant-new-conversation").click();
}

async function sendAssistantMessage(
  page: Page,
  params: { message: string; mode?: "auto" | "direct_answer" | "tool_task" | "workflow_task" }
): Promise<void> {
  await page.getByTestId("assistant-mode-select").selectOption(params.mode || "auto");
  await page.getByTestId("assistant-input-message").fill(params.message);
  await page.getByTestId("assistant-send-button").click();
}

async function refreshAssistant(page: Page): Promise<void> {
  await page.getByTestId("assistant-refresh").click();
  await page.waitForTimeout(1000);
}

async function latestTaskIdFromResponse(page: Page): Promise<string> {
  const card = page.getByTestId("assistant-last-task-card");
  await expect(card).toBeVisible({ timeout: 60_000 });
  const taskId = (await card.locator(".mono").first().textContent())?.trim() || "";
  expect(taskId).not.toEqual("");
  return taskId;
}

async function openTaskTrace(page: Page, taskId: string): Promise<void> {
  const openButtonId = `assistant-task-open-${safeId(taskId)}`;
  await expect
    .poll(
      async () => {
        await refreshAssistant(page);
        return await page.getByTestId(openButtonId).count();
      },
      { timeout: 90_000 }
    )
    .toBeGreaterThan(0);
  await page.getByTestId(openButtonId).click();
  await expect(page.getByTestId("assistant-trace-panel")).toBeVisible();
}

async function readTaskStatus(page: Page, taskId: string): Promise<string> {
  const row = page.getByTestId(`assistant-task-row-${safeId(taskId)}`);
  await expect(row).toBeVisible({ timeout: 60_000 });
  const raw = await row.locator(".status-badge").first().getAttribute("data-status");
  return String(raw || "");
}

test.describe.serial("Assistant Console E2E", () => {
  test("scenario 1: page opens and core UI is visible", async ({ page }) => {
    await login(page, USER_EMAIL, USER_PASSWORD);
    await expect(page.getByTestId("assistant-page")).toBeVisible();
    await expect(page.getByTestId("assistant-new-conversation")).toBeVisible();
    await expect(page.getByTestId("assistant-refresh")).toBeVisible();
    await expect(page.getByTestId("assistant-input-message")).toBeVisible();
    await expect(page.getByTestId("assistant-send-button")).toBeVisible();
    await expect(page.getByTestId("assistant-current-conversation")).toBeVisible();
  });

  test("scenario 2: create a direct answer task and see the response", async ({ page }) => {
    await login(page, USER_EMAIL, USER_PASSWORD);
    await resetToFreshConversation(page);
    await sendAssistantMessage(page, {
      mode: "direct_answer",
      message: uniqueMessage("What can you do for task orchestration?")
    });
    await expect(page.getByTestId("assistant-last-response")).toBeVisible({ timeout: 60_000 });
    await expect(page.getByTestId("assistant-last-route")).toHaveText("direct_answer");
    await expect(page.getByTestId("assistant-last-message")).not.toHaveText("-");
    await expect(page.getByTestId("assistant-message-history").locator(".assistant-message-item")).toHaveCount(2, {
      timeout: 30_000
    });
  });

  test("scenario 3: create a workflow task and observe status progress", async ({ page }) => {
    await login(page, USER_EMAIL, USER_PASSWORD);
    await resetToFreshConversation(page);
    await sendAssistantMessage(page, {
      mode: "workflow_task",
      message: uniqueMessage("Please prepare a research summary report for workflow observability")
    });
    await expect(page.getByTestId("assistant-last-response-type")).toHaveText("task_created", { timeout: 60_000 });
    const taskId = await latestTaskIdFromResponse(page);

    const initial = await readTaskStatus(page, taskId);
    await expect
      .poll(
        async () => {
          await refreshAssistant(page);
          return await readTaskStatus(page, taskId);
        },
        { timeout: 120_000 }
      )
      .not.toBe(initial === "QUEUED" ? "QUEUED" : "RECEIVED");

    await openTaskTrace(page, taskId);
    await expect(page.getByTestId("assistant-trace-status").locator(".status-badge")).toBeVisible();
  });

  test("scenario 4: approval workflow updates after approve action", async ({ page }) => {
    await login(page, OPERATOR_EMAIL, OPERATOR_PASSWORD);
    await resetToFreshConversation(page);
    await sendAssistantMessage(page, {
      mode: "workflow_task",
      message: uniqueMessage("send ticket to oncall team and require human approval")
    });
    await expect(page.getByTestId("assistant-last-response-type")).toHaveText("task_created", { timeout: 60_000 });
    const taskId = await latestTaskIdFromResponse(page);

    await expect
      .poll(
        async () => {
          await refreshAssistant(page);
          return await readTaskStatus(page, taskId);
        },
        { timeout: 120_000 }
      )
      .toBe("WAITING_HUMAN");

    await page.goto("/approvals");
    await expect(page.getByTestId("approvals-page")).toBeVisible();
    await expect
      .poll(
        async () => {
          await page.getByTestId("approvals-refresh").click();
          await page.waitForTimeout(1500);
          return await page.locator("[data-testid^='approval-approve-']").count();
        },
        { timeout: 90_000 }
      )
      .toBeGreaterThan(0);

    await page.locator("[data-testid^='approval-approve-']").first().click();
    await expect(page.getByTestId("toast-item-success")).toBeVisible({ timeout: 30_000 });

    await page.goto("/assistant");
    await expect(page.getByTestId("assistant-page")).toBeVisible();
    await expect
      .poll(
        async () => {
          await refreshAssistant(page);
          const count = await page.getByTestId(`assistant-task-row-${safeId(taskId)}`).count();
          if (count === 0) {
            return "MISSING";
          }
          return await readTaskStatus(page, taskId);
        },
        { timeout: 120_000 }
      )
      .not.toBe("WAITING_HUMAN");
  });

  test("scenario 5: invalid input shows an error toast", async ({ page }) => {
    await login(page, USER_EMAIL, USER_PASSWORD);
    await resetToFreshConversation(page);
    await sendAssistantMessage(page, {
      mode: "auto",
      message: "x".repeat(4105)
    });
    await expect(page.getByTestId("toast-item-error")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByTestId("toast-item-error")).toContainText("422");
  });

  test("scenario 6: task history and trace details are visible", async ({ page }) => {
    await login(page, USER_EMAIL, USER_PASSWORD);
    await resetToFreshConversation(page);
    await sendAssistantMessage(page, {
      mode: "tool_task",
      message: uniqueMessage("search temporal workflow docs for replay and trace")
    });
    await expect(page.getByTestId("assistant-last-route")).toHaveText("tool_task", { timeout: 60_000 });
    const taskId = await latestTaskIdFromResponse(page);
    await openTaskTrace(page, taskId);
    await expect(page.getByTestId("assistant-trace-plan-section")).toBeVisible();
    await expect(page.getByTestId("assistant-trace-tools-section")).toBeVisible();
    await expect(page.getByTestId("assistant-trace-steps-section")).toBeVisible();
    await expect(page.getByTestId("assistant-trace-output-section")).toBeVisible();

    const debugCount =
      (await page.locator(".assistant-tool-item").count()) + (await page.locator(".assistant-trace-item").count());
    expect(debugCount).toBeGreaterThan(0);
  });
});
