import { expect, test } from '@playwright/test'

test.use({ storageState: { cookies: [], origins: [] }, extraHTTPHeaders: {}, colorScheme: 'light', locale: 'en-US' })

const key = 'sk-bf-test-usage'
const emptyKey = 'sk-bf-no-logging'

test.beforeEach(async ({ page }) => {
  await page.route(/\/api\/governance\/virtual-keys\/(quota|usage)/, async (route) => {
    const request = route.request()
    const cors = { 'access-control-allow-origin': '*', 'access-control-allow-headers': 'x-bf-vk,content-type', 'access-control-allow-methods': 'GET,OPTIONS' }
    if (request.method() === 'OPTIONS') return route.fulfill({ status: 204, headers: cors })
    expect(request.url()).not.toContain(key)
    if (request.headers()['x-bf-vk'] === emptyKey) {
      if (request.url().includes('/quota')) return route.fulfill({ headers: cors, json: { virtual_key_name: 'Empty key', is_active: true, budgets: [], rate_limit: null, rate_limits: [], provider_configs: [], model_configs: [] } })
      return route.fulfill({ headers: cors, json: { period: '30d', logging_enabled: false, stats: null, models: [] } })
    }
    if (request.headers()['x-bf-vk'] !== key) return route.fulfill({ status: 401, headers: cors, json: { error: 'invalid key' } })
    if (request.url().includes('/quota')) {
      return route.fulfill({ headers: cors, json: {
        virtual_key_name: 'Research key', is_active: true, budgets: [], rate_limit: null, rate_limits: [], model_configs: [],
        provider_configs: [{ provider: 'openai', budgets: [{ id: 'budget', max_limit: 20, current_usage: 7.5, override_amount: 5, override_mode: 'forever', reset_duration: '1M', last_reset: '2026-11-01T00:00:00Z' }], rate_limit: null }],
      } })
    }
    return route.fulfill({ headers: cors, json: {
      period: new URL(request.url()).searchParams.get('period') || '30d', logging_enabled: true,
      start_time: '2026-10-01T00:00:00Z', end_time: '2026-11-01T00:00:00Z',
      stats: { total_requests: 12, total_tokens: 3450, prompt_tokens: 2000, completion_tokens: 1450, total_cost: 1.25 },
      models: [{ model: 'deepseek/deepseek-v3.2', provider: 'openrouter', total_requests: 12, total_tokens: 3450, total_cost: 1.25 }],
    } })
  })
})

test('a key holder sees only its usage and can clear the key', async ({ page }) => {
  await page.goto('/key-usage')
  await expect(page.getByRole('heading', { name: 'Key usage' })).toBeVisible()
  await page.getByLabel('Virtual key').fill(key)
  await page.getByRole('button', { name: 'Show password' }).click()
  await expect(page.getByLabel('Virtual key')).toHaveAttribute('type', 'text')
  await page.getByRole('button', { name: 'View usage' }).click()
  await expect(page.getByText('Research key')).toBeVisible()
  await expect(page.getByText('deepseek/deepseek-v3.2')).toBeVisible()
  await expect(page.getByText('$17.50')).toBeVisible()
  await page.getByRole('button', { name: '7 days' }).click()
  await expect(page.getByRole('button', { name: '7 days' })).toHaveAttribute('aria-pressed', 'true')
  expect(await page.evaluate((secret) => JSON.stringify({ local: localStorage, session: sessionStorage, url: location.href }).includes(secret), key)).toBe(false)
  await page.getByRole('button', { name: 'Use another key' }).click()
  await expect(page.getByLabel('Virtual key')).toHaveValue('')
  await expect(page.getByLabel('Virtual key')).toHaveAttribute('type', 'password')
  await expect(page.getByText('Research key')).toBeHidden()
  await page.getByLabel('Virtual key').fill('wrong-key')
  await page.getByRole('button', { name: 'View usage' }).click()
  await expect(page.getByRole('alert')).toBeVisible()
  await expect(page.getByText('Research key')).toBeHidden()
})

test('the page remains usable at a narrow viewport', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page.goto('/key-usage')
  await page.getByLabel('Virtual key').fill(key)
  await page.getByRole('button', { name: 'View usage' }).click()
  await expect(page.getByText('Research key')).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true)
})

test('the page is available in Russian', async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem('bifrost.locale', 'ru'))
  await page.goto('/key-usage')
  await expect(page.getByRole('heading', { name: 'Использование ключа' })).toBeVisible()
  await page.getByLabel('Виртуальный ключ').fill(key)
  await page.getByRole('button', { name: 'Показать статистику' }).click()
  await expect(page.getByRole('heading', { name: 'Топ-20 моделей' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Лимиты провайдеров' })).toBeVisible()
})

test('disabled logging and missing limits do not appear as zero usage', async ({ page }) => {
  await page.goto('/key-usage')
  await page.getByLabel('Virtual key').fill(emptyKey)
  await page.getByRole('button', { name: 'View usage' }).click()
  await expect(page.getByText('Usage statistics are unavailable because logging is disabled.')).toBeVisible()
  await expect(page.getByText('No limits are configured for this key.')).toBeVisible()
  await expect(page.getByText('$0.00')).toHaveCount(0)
})

test('switching keys cancels a pending statistics request', async ({ page }) => {
  let release: () => void = () => {}
  let requestStarted: () => void = () => {}
  const started = new Promise<void>((resolve) => { requestStarted = resolve })
  const pending = new Promise<void>((resolve) => { release = resolve })
  await page.route(/\/api\/governance\/virtual-keys\/usage/, async (route) => {
    requestStarted()
    await pending
    await route.fallback().catch(() => {})
  })
  await page.goto('/key-usage')
  await page.getByLabel('Virtual key').fill(key)
  await page.getByRole('button', { name: 'View usage' }).click()
  await started
  await page.getByRole('button', { name: 'Use another key' }).click()
  release()
  await expect(page.getByLabel('Virtual key')).toHaveValue('')
  await expect(page.getByText('Research key')).toBeHidden()
  await expect(page.getByText('deepseek/deepseek-v3.2')).toBeHidden()
})
