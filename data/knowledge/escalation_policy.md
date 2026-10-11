# Escalation Policy

**Company:** NovaShop  
**Version:** 1.5  
**Effective Date:** 2025-01-01  
**Policy Type:** Escalation  

---

## Overview

This policy defines when a customer support case must be escalated to a human agent or a specialized team, and the steps for doing so.

---

## Escalation Triggers — Automatic

The following conditions require **immediate escalation** to a human agent without attempting automated resolution:

| Trigger | Reason |
|---------|--------|
| Customer explicitly requests a human agent | Customer preference |
| Refund amount exceeds $500 | Financial risk threshold |
| Legal threat or mention of lawsuit | Legal team involvement required |
| Suspected fraud or unauthorized account access | Security team required |
| Customer reports physical injury from product | Legal and safety team required |
| Three or more failed automated resolution attempts | Escalation after repeated failure |
| Duplicate charge not resolvable via payment tool | Finance team required |

---

## Escalation Triggers — Agent-Initiated

A support agent (or AI agent) may escalate based on judgment when:

- The customer's issue requires cross-team coordination.
- Policy does not clearly cover the customer's specific situation.
- The customer is highly distressed or using threatening language.
- Resolution requires manual database access not available through standard tools.
- The agent confidence score for a proposed resolution falls below **0.75**.

---

## Escalation Levels

| Level | Team | Response SLA |
|-------|------|-------------|
| L1 | Senior Support Agent | 4 hours |
| L2 | Specialist Team (Finance / Damage / Logistics) | 8 hours |
| L3 | Management / Legal | 24 hours |

---

## Escalation Process

1. The AI agent marks the ticket status as `escalated`.
2. A human-readable escalation note is attached (includes: reason, data gathered, attempted resolutions).
3. The ticket is routed to the appropriate team queue.
4. The customer receives a notification: *"Your case has been assigned to a specialist who will contact you within [SLA]."*
5. The ticket remains open until a human agent resolves and closes it.

---

## What the AI Agent Must Include in the Escalation Note

- Ticket ID and workflow ID.
- Customer message (original).
- Data retrieved: order status, payment status, shipment status.
- Policies retrieved from RAG and their relevance scores.
- Proposed resolution (if any) and why it was not applied.
- Escalation reason (must match one of the triggers above).

---

## Post-Escalation

- The human agent has full access to the AI agent's workflow history.
- The AI agent does not continue processing the ticket after escalation.
- If the human agent determines automated resolution was sufficient, they may revert and apply it directly.

---

## Sensitive Data During Escalation

- Do not include payment card numbers or CVV in escalation notes.
- Do not include passwords or authentication tokens.
- Mask all but the last 4 digits of any card number (e.g., `**** **** **** 4242`).
