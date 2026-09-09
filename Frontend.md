# AeroLLM Frontend — Agent Build Handoff

## Goal

Build a clean, fast Astro + React frontend for AeroLLM that is friendly for end users and operators. No phase jargon, no internal project narrative. Use the backend API and this spec; do not invent routes.

## Tech Stack

- Astro with React
- TypeScript
- Tailwind CSS
- Fetch API or lightweight HTTP client
- No auth framework required for v1 unless the backend enforces it

## Design Direction

- Dark-first, modern dashboard aesthetic
- High readability, low visual noise
- Cards and tables for lists; simple status badges
- One clear action per view
- Fully responsive down to mobile

## Backend Base URL

Use a configurable base URL. For local development, default to `http://localhost:8080`.

## API Surface

Use these endpoints. Do not call internal-only paths.

### Core

- `GET /healthz`
- `GET /readyz`
- `POST /v1/chat/completions`
- `GET /mcp`
- `POST /mcp`

### Operations

- `GET /resilience/status`
- `GET /backpressure/status`
- `GET /v1/slo/budget`
- `GET /v1/audit/events`
- `POST /v1/meter/usage`
- `POST /v1/admission/validate`

### Management

- `GET /v1/flags`
- `POST /v1/flags`
- `POST /v1/policy`
- `GET /v1/policy`
- `POST /v1/policy/block`
- `POST /v1/retention`
- `POST /v1/incidents`
- `GET /v1/incidents`
- `PUT /v1/incidents`
- `POST /v1/notification/channels`
- `GET /v1/notification/channels`
- `POST /v1/notification/subscriptions`
- `POST /v1/schedule`
- `GET /v1/schedule`
- `PUT /v1/schedule`
- `POST /v1/secrets`
- `GET /v1/secrets`
- `DELETE /v1/secrets`
- `POST /v1/region/regions`
- `GET /v1/region/regions`
- `POST /v1/region/residency`
- `POST /v1/region/routes`

## Required Pages

- Home / Dashboard
- Chat
- Health
- MCP
- Flags
- Policy
- Incidents
- Notifications
- Schedule
- Secrets
- Regions

## Page Contracts

### Home / Dashboard

Show:

- healthz and readyz status
- resilience status summary
- backpressure status summary
- recent audit events count
- quick links to primary actions

### Chat

- model selector
- message history
- streaming response render
- cancel generation button when streaming is active

### Health

- healthz status
- readyz status
- raw JSON viewer

### MCP

- initialize button
- tools list viewer
- raw request/response JSON panels

### Flags

- create/update form
- flag list
- enable/disable toggle

### Policy

- create policy form
- policy list
- block middleware trigger form

### Incidents

- create incident form
- incident list with severity and status badges
- update status form

### Notifications

- create channel form
- channel list
- create subscription form
- subscription list

### Schedule

- create task form
- task list
- update status form

### Secrets

- create secret form
- secret list
- delete action

### Regions

- create region form
- region list
- create residency policy form
- create route rule form

## Components

- `Layout.astro` with nav sidebar
- `Card`, `Badge`, `StatusPill`
- `JsonViewer`
- `ApiErrorBoundary`
- `ConfirmButton`

## State

- Local component state is fine for v1
- Keep forms simple with controlled inputs
- Show loading and error states on every action

## Auth

- If backend requires auth, add an API key input in app settings
- Send API key as `Authorization: Bearer <key>` or required header
- Do not implement OAuth unless backend supports it

## Responsiveness

- Mobile-first layout
- Collapsible sidebar on small screens
- Tables become card stacks on mobile

## Accessibility

- Semantic HTML
- Focus management
- Keyboard navigable forms
- Color contrast compliance

## Performance

- Astro static shell with React islands
- Minimal JS per page
- Lazy load non-critical views

## File Structure

```
src/
  pages/
    index.astro
    chat.astro
    health.astro
    mcp.astro
    flags.astro
    policy.astro
    incidents.astro
    notifications.astro
    schedule.astro
    secrets.astro
    regions.astro
  components/
    Layout.astro
    Card.astro
    Badge.astro
    StatusPill.astro
    JsonViewer.astro
    ApiErrorBoundary.astro
    ConfirmButton.astro
  lib/
    api.ts
```

## Acceptance Criteria

- All listed pages render
- Each page can read from and write to its mapped API endpoints
- Mobile responsive
- Dark theme
- No build errors
- `npm run preview` works after `npm run build`
