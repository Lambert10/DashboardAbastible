/* global process */

import 'dotenv/config'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Buffer } from 'node:buffer'
import { fileURLToPath } from 'node:url'
import cors from 'cors'
import Database from 'better-sqlite3'
import express from 'express'
import pg from 'pg'

const PORT = Number(process.env.PORT ?? process.env.API_PORT ?? 4011)
const MAX_DAILY_HISTORY_RECORDS = 730
const STREAK_API_BASE_URL = process.env.STREAK_API_BASE_URL ?? 'https://api.streak.com'
const DEFAULT_DASHBOARD_HISTORY_START_DAY_KEY = '2025-12-15'
const DEFAULT_DASHBOARD_LEGACY_END_DAY_KEY = '2026-02-25'
const DEFAULT_DASHBOARD_OFFICIAL_START_DAY_KEY = '2026-03-04'
const DEFAULT_DASHBOARD_OFFICIAL_TOTAL_PROVIDERS = 1420
const DEFAULT_DASHBOARD_OFFICIAL_TOTAL_TOLERANCE = 1
const DEFAULT_DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS = 3000
const KNOWN_TOTAL_ANOMALIES = new Set(['2026-03-06:1419'])
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const { Pool } = pg
const databaseUrl = String(process.env.DATABASE_URL ?? '').trim()
const usePostgres = Boolean(databaseUrl)
const dataDirectoryPath = path.join(__dirname, 'data')
const defaultDatabasePath = path.join(dataDirectoryPath, 'dashboard-history.sqlite')
const requestedDatabasePath = String(process.env.DASHBOARD_DB_PATH ?? '').trim() || defaultDatabasePath

let databasePath = ''
let database = null
let pgPool = null
let storageLabel = ''

function resolveDatabasePath(requestedPath) {
  const fallbackDatabasePath = path.join(os.tmpdir(), 'dashboard-history.sqlite')

  try {
    fs.mkdirSync(path.dirname(requestedPath), { recursive: true })
    return requestedPath
  } catch (error) {
    if (requestedPath === fallbackDatabasePath) {
      throw error
    }

    fs.mkdirSync(path.dirname(fallbackDatabasePath), { recursive: true })
    console.warn(
      `[dashboard-api] No se pudo usar DASHBOARD_DB_PATH (${requestedPath}). Se usara ruta temporal: ${fallbackDatabasePath}.`,
    )
    return fallbackDatabasePath
  }
}

function resolvePostgresSslConfig() {
  const mode = String(process.env.PGSSLMODE ?? '').trim().toLowerCase()
  if (mode === 'disable') {
    return false
  }

  if (databaseUrl.includes('localhost') || databaseUrl.includes('127.0.0.1')) {
    return false
  }

  return { rejectUnauthorized: false }
}

if (usePostgres) {
  pgPool = new Pool({
    connectionString: databaseUrl,
    ssl: resolvePostgresSslConfig(),
  })
  databasePath = 'postgres'
  storageLabel = 'postgres'
} else {
  databasePath = resolveDatabasePath(requestedDatabasePath)
  database = new Database(databasePath)
  database.pragma('journal_mode = WAL')
  storageLabel = `sqlite:${databasePath}`
}

let selectAllSnapshotsStatement = null
let upsertSnapshotStatement = null
let deleteSnapshotsStatement = null
let deletePayloadsStatement = null
let upsertPayloadStatement = null
let selectPayloadByDayKeyStatement = null
let selectPayloadDayKeysStatement = null

function initializeSqliteStatements() {
  database.exec(`
    CREATE TABLE IF NOT EXISTS daily_history_snapshots (
      day_key TEXT PRIMARY KEY,
      file_name TEXT NOT NULL DEFAULT '',
      sheet_name TEXT NOT NULL DEFAULT '',
      total_providers INTEGER NOT NULL DEFAULT 0,
      contacted_providers INTEGER NOT NULL DEFAULT 0,
      trained_providers INTEGER NOT NULL DEFAULT 0,
      enrolled_providers INTEGER NOT NULL DEFAULT 0,
      rescued_providers INTEGER NOT NULL DEFAULT 0,
      cited_providers INTEGER NOT NULL DEFAULT 0,
      training_days_count INTEGER NOT NULL DEFAULT 0,
      contact_rate REAL NOT NULL DEFAULT 0,
      trained_rate REAL NOT NULL DEFAULT 0,
      rescue_rate REAL NOT NULL DEFAULT 0,
      saved_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
  `)

  database.exec(`
    CREATE TABLE IF NOT EXISTS daily_history_payloads (
      day_key TEXT PRIMARY KEY,
      file_name TEXT NOT NULL DEFAULT '',
      sheet_name TEXT NOT NULL DEFAULT '',
      headers_json TEXT NOT NULL DEFAULT '[]',
      rows_json TEXT NOT NULL DEFAULT '[]',
      mapping_json TEXT NOT NULL DEFAULT '{}',
      contact_stage TEXT NOT NULL DEFAULT '',
      trained_stage TEXT NOT NULL DEFAULT '',
      saved_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
  `)

  selectAllSnapshotsStatement = database.prepare(`
    SELECT
      day_key AS dayKey,
      file_name AS fileName,
      sheet_name AS sheetName,
      total_providers AS totalProviders,
      contacted_providers AS contactedProviders,
      trained_providers AS trainedProviders,
      enrolled_providers AS enrolledProviders,
      rescued_providers AS rescuedProviders,
      cited_providers AS citedProviders,
      training_days_count AS trainingDaysCount,
      contact_rate AS contactRate,
      trained_rate AS trainedRate,
      rescue_rate AS rescueRate,
      saved_at AS savedAt
    FROM daily_history_snapshots
    ORDER BY day_key DESC
    LIMIT ?
  `)

  upsertSnapshotStatement = database.prepare(`
    INSERT INTO daily_history_snapshots (
      day_key,
      file_name,
      sheet_name,
      total_providers,
      contacted_providers,
      trained_providers,
      enrolled_providers,
      rescued_providers,
      cited_providers,
      training_days_count,
      contact_rate,
      trained_rate,
      rescue_rate,
      saved_at
    ) VALUES (
      @dayKey,
      @fileName,
      @sheetName,
      @totalProviders,
      @contactedProviders,
      @trainedProviders,
      @enrolledProviders,
      @rescuedProviders,
      @citedProviders,
      @trainingDaysCount,
      @contactRate,
      @trainedRate,
      @rescueRate,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(day_key) DO UPDATE SET
      file_name = excluded.file_name,
      sheet_name = excluded.sheet_name,
      total_providers = excluded.total_providers,
      contacted_providers = excluded.contacted_providers,
      trained_providers = excluded.trained_providers,
      enrolled_providers = excluded.enrolled_providers,
      rescued_providers = excluded.rescued_providers,
      cited_providers = excluded.cited_providers,
      training_days_count = excluded.training_days_count,
      contact_rate = excluded.contact_rate,
      trained_rate = excluded.trained_rate,
      rescue_rate = excluded.rescue_rate,
      saved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  `)

  deleteSnapshotsStatement = database.prepare('DELETE FROM daily_history_snapshots')
  deletePayloadsStatement = database.prepare('DELETE FROM daily_history_payloads')

  upsertPayloadStatement = database.prepare(`
    INSERT INTO daily_history_payloads (
      day_key,
      file_name,
      sheet_name,
      headers_json,
      rows_json,
      mapping_json,
      contact_stage,
      trained_stage,
      saved_at
    ) VALUES (
      @dayKey,
      @fileName,
      @sheetName,
      @headersJson,
      @rowsJson,
      @mappingJson,
      @contactStage,
      @trainedStage,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(day_key) DO UPDATE SET
      file_name = excluded.file_name,
      sheet_name = excluded.sheet_name,
      headers_json = excluded.headers_json,
      rows_json = excluded.rows_json,
      mapping_json = excluded.mapping_json,
      contact_stage = excluded.contact_stage,
      trained_stage = excluded.trained_stage,
      saved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  `)

  selectPayloadByDayKeyStatement = database.prepare(`
    SELECT
      day_key AS dayKey,
      file_name AS fileName,
      sheet_name AS sheetName,
      headers_json AS headersJson,
      rows_json AS rowsJson,
      mapping_json AS mappingJson,
      contact_stage AS contactStage,
      trained_stage AS trainedStage,
      saved_at AS savedAt
    FROM daily_history_payloads
    WHERE day_key = ?
  `)

  selectPayloadDayKeysStatement = database.prepare(`
    SELECT day_key AS dayKey
    FROM daily_history_payloads
  `)
}

async function initializePostgresTables() {
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS daily_history_snapshots (
      day_key TEXT PRIMARY KEY,
      file_name TEXT NOT NULL DEFAULT '',
      sheet_name TEXT NOT NULL DEFAULT '',
      total_providers INTEGER NOT NULL DEFAULT 0,
      contacted_providers INTEGER NOT NULL DEFAULT 0,
      trained_providers INTEGER NOT NULL DEFAULT 0,
      enrolled_providers INTEGER NOT NULL DEFAULT 0,
      rescued_providers INTEGER NOT NULL DEFAULT 0,
      cited_providers INTEGER NOT NULL DEFAULT 0,
      training_days_count INTEGER NOT NULL DEFAULT 0,
      contact_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
      trained_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
      rescue_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
      saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `)

  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS daily_history_payloads (
      day_key TEXT PRIMARY KEY,
      file_name TEXT NOT NULL DEFAULT '',
      sheet_name TEXT NOT NULL DEFAULT '',
      headers_json TEXT NOT NULL DEFAULT '[]',
      rows_json TEXT NOT NULL DEFAULT '[]',
      mapping_json TEXT NOT NULL DEFAULT '{}',
      contact_stage TEXT NOT NULL DEFAULT '',
      trained_stage TEXT NOT NULL DEFAULT '',
      saved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `)
}

if (usePostgres) {
  await initializePostgresTables()
} else {
  initializeSqliteStatements()
}

function toSafeInteger(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return 0
  }

  return Math.max(0, Math.round(numeric))
}

function toSafeRate(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return 0
  }

  return Number(numeric.toFixed(1))
}

function isValidDayKey(dayKey) {
  if (typeof dayKey !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(dayKey)) {
    return false
  }

  const [yearRaw, monthRaw, dayRaw] = dayKey.split('-')
  const year = Number(yearRaw)
  const month = Number(monthRaw)
  const day = Number(dayRaw)

  const parsed = new Date(Date.UTC(year, month - 1, day))
  return (
    parsed.getUTCFullYear() === year &&
    parsed.getUTCMonth() + 1 === month &&
    parsed.getUTCDate() === day
  )
}

async function dbReadAllSnapshots(limit) {
  if (usePostgres) {
    const result = await pgPool.query(
      `
      SELECT
        day_key AS "dayKey",
        file_name AS "fileName",
        sheet_name AS "sheetName",
        total_providers AS "totalProviders",
        contacted_providers AS "contactedProviders",
        trained_providers AS "trainedProviders",
        enrolled_providers AS "enrolledProviders",
        rescued_providers AS "rescuedProviders",
        cited_providers AS "citedProviders",
        training_days_count AS "trainingDaysCount",
        contact_rate AS "contactRate",
        trained_rate AS "trainedRate",
        rescue_rate AS "rescueRate",
        saved_at AS "savedAt"
      FROM daily_history_snapshots
      ORDER BY day_key DESC
      LIMIT $1
      `,
      [limit],
    )
    return result.rows
  }

  return selectAllSnapshotsStatement.all(limit)
}

async function dbUpsertSnapshot(snapshot) {
  if (usePostgres) {
    await pgPool.query(
      `
      INSERT INTO daily_history_snapshots (
        day_key,
        file_name,
        sheet_name,
        total_providers,
        contacted_providers,
        trained_providers,
        enrolled_providers,
        rescued_providers,
        cited_providers,
        training_days_count,
        contact_rate,
        trained_rate,
        rescue_rate,
        saved_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW()
      )
      ON CONFLICT(day_key) DO UPDATE SET
        file_name = EXCLUDED.file_name,
        sheet_name = EXCLUDED.sheet_name,
        total_providers = EXCLUDED.total_providers,
        contacted_providers = EXCLUDED.contacted_providers,
        trained_providers = EXCLUDED.trained_providers,
        enrolled_providers = EXCLUDED.enrolled_providers,
        rescued_providers = EXCLUDED.rescued_providers,
        cited_providers = EXCLUDED.cited_providers,
        training_days_count = EXCLUDED.training_days_count,
        contact_rate = EXCLUDED.contact_rate,
        trained_rate = EXCLUDED.trained_rate,
        rescue_rate = EXCLUDED.rescue_rate,
        saved_at = NOW()
      `,
      [
        snapshot.dayKey,
        snapshot.fileName,
        snapshot.sheetName,
        snapshot.totalProviders,
        snapshot.contactedProviders,
        snapshot.trainedProviders,
        snapshot.enrolledProviders,
        snapshot.rescuedProviders,
        snapshot.citedProviders,
        snapshot.trainingDaysCount,
        snapshot.contactRate,
        snapshot.trainedRate,
        snapshot.rescueRate,
      ],
    )
    return
  }

  upsertSnapshotStatement.run(snapshot)
}

async function dbDeleteSnapshots() {
  if (usePostgres) {
    await pgPool.query('DELETE FROM daily_history_snapshots')
    return
  }

  deleteSnapshotsStatement.run()
}

async function dbDeletePayloads() {
  if (usePostgres) {
    await pgPool.query('DELETE FROM daily_history_payloads')
    return
  }

  deletePayloadsStatement.run()
}

async function dbUpsertPayload(payload) {
  if (usePostgres) {
    await pgPool.query(
      `
      INSERT INTO daily_history_payloads (
        day_key,
        file_name,
        sheet_name,
        headers_json,
        rows_json,
        mapping_json,
        contact_stage,
        trained_stage,
        saved_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,NOW()
      )
      ON CONFLICT(day_key) DO UPDATE SET
        file_name = EXCLUDED.file_name,
        sheet_name = EXCLUDED.sheet_name,
        headers_json = EXCLUDED.headers_json,
        rows_json = EXCLUDED.rows_json,
        mapping_json = EXCLUDED.mapping_json,
        contact_stage = EXCLUDED.contact_stage,
        trained_stage = EXCLUDED.trained_stage,
        saved_at = NOW()
      `,
      [
        payload.dayKey,
        payload.fileName,
        payload.sheetName,
        payload.headersJson,
        payload.rowsJson,
        payload.mappingJson,
        payload.contactStage,
        payload.trainedStage,
      ],
    )
    return
  }

  upsertPayloadStatement.run(payload)
}

async function dbReadPayloadByDayKey(dayKey) {
  if (usePostgres) {
    const result = await pgPool.query(
      `
      SELECT
        day_key AS "dayKey",
        file_name AS "fileName",
        sheet_name AS "sheetName",
        headers_json AS "headersJson",
        rows_json AS "rowsJson",
        mapping_json AS "mappingJson",
        contact_stage AS "contactStage",
        trained_stage AS "trainedStage",
        saved_at AS "savedAt"
      FROM daily_history_payloads
      WHERE day_key = $1
      `,
      [dayKey],
    )
    return result.rows[0] ?? null
  }

  return selectPayloadByDayKeyStatement.get(dayKey)
}

async function dbReadPayloadDayKeys() {
  if (usePostgres) {
    const result = await pgPool.query(`SELECT day_key AS "dayKey" FROM daily_history_payloads`)
    return result.rows
  }

  return selectPayloadDayKeysStatement.all()
}

function resolveDayKeyEnv(variableName, fallbackDayKey) {
  const configured = String(process.env[variableName] ?? fallbackDayKey).trim()
  return isValidDayKey(configured) ? configured : fallbackDayKey
}

function resolveHistoryStartDayKey() {
  return resolveDayKeyEnv('DASHBOARD_HISTORY_START_DAY_KEY', DEFAULT_DASHBOARD_HISTORY_START_DAY_KEY)
}

function resolveLegacyEndDayKey() {
  return resolveDayKeyEnv('DASHBOARD_LEGACY_END_DAY_KEY', DEFAULT_DASHBOARD_LEGACY_END_DAY_KEY)
}

function resolveOfficialStartDayKey() {
  return resolveDayKeyEnv('DASHBOARD_OFFICIAL_START_DAY_KEY', DEFAULT_DASHBOARD_OFFICIAL_START_DAY_KEY)
}

function resolveOfficialTotalProviders() {
  const configured = Number(
    process.env.DASHBOARD_OFFICIAL_TOTAL_PROVIDERS ?? DEFAULT_DASHBOARD_OFFICIAL_TOTAL_PROVIDERS,
  )
  if (!Number.isFinite(configured) || configured <= 0) {
    return DEFAULT_DASHBOARD_OFFICIAL_TOTAL_PROVIDERS
  }

  return Math.round(configured)
}

function resolveOfficialTotalTolerance() {
  const configured = Number(
    process.env.DASHBOARD_OFFICIAL_TOTAL_TOLERANCE ?? DEFAULT_DASHBOARD_OFFICIAL_TOTAL_TOLERANCE,
  )
  if (!Number.isFinite(configured) || configured < 0) {
    return DEFAULT_DASHBOARD_OFFICIAL_TOTAL_TOLERANCE
  }

  return Math.round(configured)
}

function resolveLegacyMinTotalProviders() {
  const configured = Number(
    process.env.DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS ?? DEFAULT_DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS,
  )
  if (!Number.isFinite(configured) || configured <= 0) {
    return DEFAULT_DASHBOARD_LEGACY_MIN_TOTAL_PROVIDERS
  }

  return Math.round(configured)
}

function classifyTimelineSegment(snapshot) {
  const dayKey = String(snapshot?.dayKey ?? '')
  const totalProviders = toSafeInteger(snapshot?.totalProviders)
  const legacyEndDayKey = resolveLegacyEndDayKey()
  const officialStartDayKey = resolveOfficialStartDayKey()
  const officialTotalProviders = resolveOfficialTotalProviders()
  const officialTotalTolerance = resolveOfficialTotalTolerance()
  const legacyMinTotalProviders = resolveLegacyMinTotalProviders()

  if (totalProviders >= legacyMinTotalProviders) {
    return {
      segment: 'legacy',
      reason: `Universo >= ${legacyMinTotalProviders} proveedores.`,
    }
  }

  if (Math.abs(totalProviders - officialTotalProviders) <= officialTotalTolerance) {
    const anomalyKey = `${dayKey}:${totalProviders}`
    if (KNOWN_TOTAL_ANOMALIES.has(anomalyKey)) {
      return {
        segment: 'official',
        reason: `Anomalia conocida (${totalProviders}) aceptada para ${dayKey}.`,
      }
    }

    return {
      segment: 'official',
      reason: `Universo objetivo ${officialTotalProviders} +/- ${officialTotalTolerance}.`,
    }
  }

  if (isValidDayKey(dayKey) && dayKey <= legacyEndDayKey) {
    return {
      segment: 'legacy',
      reason: `Fecha <= fin legacy (${legacyEndDayKey}).`,
    }
  }

  if (isValidDayKey(dayKey) && dayKey >= officialStartDayKey) {
    return {
      segment: 'official',
      reason: `Fecha >= inicio oficial (${officialStartDayKey}).`,
    }
  }

  const midpoint = Math.round((officialTotalProviders + legacyMinTotalProviders) / 2)
  if (totalProviders >= midpoint) {
    return {
      segment: 'legacy',
      reason: `Periodo intermedio: total ${totalProviders} mas cercano a universo legacy.`,
    }
  }

  return {
    segment: 'official',
    reason: `Periodo intermedio: total ${totalProviders} mas cercano a universo oficial.`,
  }
}

function normalizeSnapshotPayload(dayKey, payload) {
  return {
    dayKey,
    fileName: String(payload?.fileName ?? ''),
    sheetName: String(payload?.sheetName ?? ''),
    totalProviders: toSafeInteger(payload?.totalProviders),
    contactedProviders: toSafeInteger(payload?.contactedProviders),
    trainedProviders: toSafeInteger(payload?.trainedProviders),
    enrolledProviders: toSafeInteger(payload?.enrolledProviders),
    rescuedProviders: toSafeInteger(payload?.rescuedProviders),
    citedProviders: toSafeInteger(payload?.citedProviders),
    trainingDaysCount: toSafeInteger(payload?.trainingDaysCount),
    contactRate: toSafeRate(payload?.contactRate),
    trainedRate: toSafeRate(payload?.trainedRate),
    rescueRate: toSafeRate(payload?.rescueRate),
  }
}

function sanitizePayloadRows(rows) {
  if (!Array.isArray(rows)) {
    return []
  }

  return rows
    .filter((row) => row && typeof row === 'object' && !Array.isArray(row))
    .slice(0, 20000)
    .map((row) => {
      const cleanRow = {}
      Object.entries(row).forEach(([header, value]) => {
        cleanRow[String(header)] = value
      })
      return cleanRow
    })
}

function sanitizePayloadHeaders(headers, rows) {
  if (Array.isArray(headers)) {
    const cleanHeaders = headers
      .map((header) => String(header).trim())
      .filter(Boolean)
      .slice(0, 300)
    if (cleanHeaders.length) {
      return cleanHeaders
    }
  }

  const headerSet = new Set()
  rows.forEach((row) => {
    Object.keys(row).forEach((header) => headerSet.add(String(header)))
  })
  return Array.from(headerSet).slice(0, 300)
}

function sanitizePayloadMapping(mapping) {
  if (!mapping || typeof mapping !== 'object' || Array.isArray(mapping)) {
    return {}
  }

  const cleanMapping = {}
  Object.entries(mapping).forEach(([field, column]) => {
    cleanMapping[String(field)] = String(column ?? '')
  })
  return cleanMapping
}

function normalizeDashboardPayload(dayKey, payload, fallbackSnapshot) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    return null
  }

  const rows = sanitizePayloadRows(payload.rows)
  const headers = sanitizePayloadHeaders(payload.headers, rows)
  const mapping = sanitizePayloadMapping(payload.mapping)

  if (!rows.length || !headers.length) {
    return null
  }

  return {
    dayKey,
    fileName: String(payload.fileName ?? fallbackSnapshot?.fileName ?? ''),
    sheetName: String(payload.sheetName ?? fallbackSnapshot?.sheetName ?? ''),
    headersJson: JSON.stringify(headers),
    rowsJson: JSON.stringify(rows),
    mappingJson: JSON.stringify(mapping),
    contactStage: String(payload.contactStage ?? ''),
    trainedStage: String(payload.trainedStage ?? ''),
  }
}

function safeParseJson(text, fallbackValue) {
  try {
    return JSON.parse(String(text ?? ''))
  } catch {
    return fallbackValue
  }
}

async function readDashboardPayload(dayKey) {
  const row = await dbReadPayloadByDayKey(dayKey)
  if (!row) {
    return null
  }

  const parsedHeaders = safeParseJson(row.headersJson, [])
  const parsedRows = safeParseJson(row.rowsJson, [])
  const headers = Array.isArray(parsedHeaders) ? parsedHeaders : []
  const rows = Array.isArray(parsedRows) ? parsedRows : []
  const mappingRaw = safeParseJson(row.mappingJson, {})
  const mapping =
    mappingRaw && typeof mappingRaw === 'object' && !Array.isArray(mappingRaw) ? mappingRaw : {}

  return {
    dayKey: String(row.dayKey),
    fileName: String(row.fileName ?? ''),
    sheetName: String(row.sheetName ?? ''),
    headers: headers.map((header) => String(header)),
    rows: rows.filter((item) => item && typeof item === 'object' && !Array.isArray(item)),
    mapping: Object.fromEntries(
      Object.entries(mapping).map(([field, column]) => [String(field), String(column ?? '')]),
    ),
    contactStage: String(row.contactStage ?? ''),
    trainedStage: String(row.trainedStage ?? ''),
    savedAt: String(row.savedAt ?? ''),
  }
}

async function readPayloadDayKeySet() {
  const rows = await dbReadPayloadDayKeys()
  return new Set(
    rows
      .map((row) => String(row.dayKey ?? ''))
      .filter((dayKey) => isValidDayKey(dayKey)),
  )
}

async function readAllSnapshots() {
  const rows = await dbReadAllSnapshots(MAX_DAILY_HISTORY_RECORDS)
  return rows.reverse()
}

function sanitizeSnapshotForModel(snapshot) {
  const totalProviders = toSafeInteger(snapshot.totalProviders)
  const contactedProviders = toSafeInteger(snapshot.contactedProviders)
  const trainedProviders = toSafeInteger(snapshot.trainedProviders)
  const enrolledProviders = toSafeInteger(snapshot.enrolledProviders)
  const rescuedProviders = toSafeInteger(snapshot.rescuedProviders)
  const citedProviders = toSafeInteger(snapshot.citedProviders)
  const trainingDaysCount = toSafeInteger(snapshot.trainingDaysCount)

  const normalizedContacted = Math.max(contactedProviders, trainedProviders)
  const normalizedTotal = Math.max(
    totalProviders,
    normalizedContacted,
    trainedProviders,
    enrolledProviders,
    rescuedProviders,
  )

  return {
    dayKey: String(snapshot.dayKey),
    fileName: String(snapshot.fileName ?? ''),
    sheetName: String(snapshot.sheetName ?? ''),
    totalProviders: normalizedTotal,
    contactedProviders: normalizedContacted,
    trainedProviders,
    enrolledProviders,
    rescuedProviders,
    citedProviders,
    trainingDaysCount,
    contactRate: normalizedTotal ? Number(((normalizedContacted / normalizedTotal) * 100).toFixed(1)) : 0,
    trainedRate: normalizedTotal ? Number(((trainedProviders / normalizedTotal) * 100).toFixed(1)) : 0,
    rescueRate: normalizedTotal ? Number(((rescuedProviders / normalizedTotal) * 100).toFixed(1)) : 0,
    savedAt: String(snapshot.savedAt ?? new Date().toISOString()),
  }
}

function normalizeSnapshotsForTimeline(snapshots) {
  const historyStartDayKey = resolveHistoryStartDayKey()
  const ordered = [...snapshots]
    .filter((snapshot) => isValidDayKey(String(snapshot.dayKey ?? '')))
    .filter((snapshot) => String(snapshot.dayKey) >= historyStartDayKey)
    .map(sanitizeSnapshotForModel)
    .sort((a, b) => a.dayKey.localeCompare(b.dayKey))

  const normalized = []
  const previousBySegment = new Map()

  ordered.forEach((snapshot) => {
    const timeline = classifyTimelineSegment(snapshot)
    const previousInSegment = previousBySegment.get(timeline.segment)
    const normalizedSnapshot = {
      ...snapshot,
      timelineSegment: timeline.segment,
      timelineReason: timeline.reason,
    }

    if (previousInSegment) {
      normalizedSnapshot.contactedProviders = Math.max(
        normalizedSnapshot.contactedProviders,
        previousInSegment.contactedProviders,
      )
      normalizedSnapshot.trainedProviders = Math.max(
        normalizedSnapshot.trainedProviders,
        previousInSegment.trainedProviders,
      )
      normalizedSnapshot.enrolledProviders = Math.max(
        normalizedSnapshot.enrolledProviders,
        previousInSegment.enrolledProviders,
      )
      normalizedSnapshot.rescuedProviders = Math.max(
        normalizedSnapshot.rescuedProviders,
        previousInSegment.rescuedProviders,
      )
      normalizedSnapshot.trainingDaysCount = Math.max(
        normalizedSnapshot.trainingDaysCount,
        previousInSegment.trainingDaysCount,
      )
    }

    normalizedSnapshot.contactedProviders = Math.max(
      normalizedSnapshot.contactedProviders,
      normalizedSnapshot.trainedProviders,
    )
    normalizedSnapshot.totalProviders = Math.max(
      normalizedSnapshot.totalProviders,
      normalizedSnapshot.contactedProviders,
      normalizedSnapshot.trainedProviders,
      normalizedSnapshot.enrolledProviders,
      normalizedSnapshot.rescuedProviders,
    )
    normalizedSnapshot.contactRate = normalizedSnapshot.totalProviders
      ? Number(((normalizedSnapshot.contactedProviders / normalizedSnapshot.totalProviders) * 100).toFixed(1))
      : 0
    normalizedSnapshot.trainedRate = normalizedSnapshot.totalProviders
      ? Number(((normalizedSnapshot.trainedProviders / normalizedSnapshot.totalProviders) * 100).toFixed(1))
      : 0
    normalizedSnapshot.rescueRate = normalizedSnapshot.totalProviders
      ? Number(((normalizedSnapshot.rescuedProviders / normalizedSnapshot.totalProviders) * 100).toFixed(1))
      : 0

    normalized.push(normalizedSnapshot)
    previousBySegment.set(timeline.segment, normalizedSnapshot)
  })

  return normalized.slice(-MAX_DAILY_HISTORY_RECORDS)
}

async function readModelSnapshots() {
  const rawSnapshots = await readAllSnapshots()
  const normalizedSnapshots = normalizeSnapshotsForTimeline(rawSnapshots)

  const payloadDayKeys = await readPayloadDayKeySet()
  return normalizedSnapshots.map((snapshot) => ({
    ...snapshot,
    hasPayload: payloadDayKeys.has(String(snapshot.dayKey ?? '')),
  }))
}

function getStreakApiKey() {
  return String(process.env.STREAK_API_KEY ?? '').trim()
}

function buildStreakAuthHeader(apiKey) {
  return `Basic ${Buffer.from(`${apiKey}:`).toString('base64')}`
}

function toEntityArray(payload, preferredKeys = []) {
  if (Array.isArray(payload)) {
    return payload
  }

  if (!payload || typeof payload !== 'object') {
    return []
  }

  for (const key of preferredKeys) {
    const value = payload[key]
    if (Array.isArray(value)) {
      return value
    }

    if (value && typeof value === 'object') {
      const values = Object.values(value).filter((item) => item && typeof item === 'object')
      if (values.length) {
        return values
      }
    }
  }

  if (
    payload.boxKey ||
    payload.pipelineKey ||
    payload.fieldKey ||
    payload.stageKey ||
    (payload.key && payload.name)
  ) {
    return [payload]
  }

  return Object.values(payload).filter((item) => item && typeof item === 'object')
}

function normalizeStreakPipeline(pipeline) {
  const key = String(pipeline?.pipelineKey ?? pipeline?.key ?? '')
  return {
    key,
    name: String(pipeline?.name ?? pipeline?.displayName ?? key),
  }
}

function normalizeStreakField(field) {
  const key = String(field?.fieldKey ?? field?.key ?? '')
  const optionByKey = {}

  const dropdownItems = Array.isArray(field?.dropdownSettings?.items)
    ? field.dropdownSettings.items
    : []
  dropdownItems.forEach((item) => {
    const optionKey = String(item?.key ?? '')
    if (!optionKey) {
      return
    }

    optionByKey[optionKey] = String(item?.name ?? optionKey)
  })

  const tagItems = Array.isArray(field?.tagSettings?.tags) ? field.tagSettings.tags : []
  tagItems.forEach((item) => {
    const optionKey = String(item?.key ?? '')
    if (!optionKey) {
      return
    }

    optionByKey[optionKey] = String(item?.tag ?? item?.name ?? optionKey)
  })

  return {
    key,
    name: String(field?.name ?? field?.label ?? key),
    type: String(field?.type ?? '').toUpperCase(),
    optionByKey,
  }
}

function normalizeStreakStage(stage) {
  const key = String(stage?.stageKey ?? stage?.key ?? '')
  return {
    key,
    name: String(stage?.name ?? stage?.label ?? key),
  }
}

function extractStreakErrorMessage(payload, fallbackMessage) {
  if (payload && typeof payload === 'object') {
    if (typeof payload.error === 'string' && payload.error.trim()) {
      return payload.error
    }

    if (typeof payload.message === 'string' && payload.message.trim()) {
      return payload.message
    }
  }

  return fallbackMessage
}

async function streakRequest(pathname, query = {}) {
  const apiKey = getStreakApiKey()
  if (!apiKey) {
    const missingKeyError = new Error('La variable STREAK_API_KEY no esta configurada en el backend.')
    missingKeyError.statusCode = 503
    throw missingKeyError
  }

  const url = new URL(pathname, STREAK_API_BASE_URL)
  Object.entries(query).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      url.searchParams.set(key, String(value))
    }
  })

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: buildStreakAuthHeader(apiKey),
      Accept: 'application/json',
    },
  })

  const rawBody = await response.text()
  let payload = {}

  if (rawBody) {
    try {
      payload = JSON.parse(rawBody)
    } catch {
      payload = { rawBody }
    }
  }

  if (!response.ok) {
    const error = new Error(
      extractStreakErrorMessage(
        payload,
        `Streak API respondio ${response.status} ${response.statusText}`.trim(),
      ),
    )
    error.statusCode = response.status
    throw error
  }

  return payload
}

async function fetchStreakPipelines() {
  const payload = await streakRequest('/api/v1/pipelines')
  const pipelines = toEntityArray(payload, ['pipelines'])
  return pipelines
    .map(normalizeStreakPipeline)
    .filter((pipeline) => pipeline.key)
    .sort((a, b) => a.name.localeCompare(b.name, 'es'))
}

async function fetchStreakPipeline(pipelineKey) {
  const payload = await streakRequest(`/api/v1/pipelines/${encodeURIComponent(pipelineKey)}`)
  return normalizeStreakPipeline(payload)
}

async function fetchStreakStages(pipelineKey) {
  const payload = await streakRequest(`/api/v1/pipelines/${encodeURIComponent(pipelineKey)}/stages`)
  return toEntityArray(payload, ['stages']).map(normalizeStreakStage).filter((stage) => stage.key)
}

async function fetchStreakFields(pipelineKey) {
  const payload = await streakRequest(`/api/v1/pipelines/${encodeURIComponent(pipelineKey)}/fields`)
  return toEntityArray(payload, ['fields']).map(normalizeStreakField).filter((field) => field.key)
}

async function fetchStreakBoxes(pipelineKey) {
  const payload = await streakRequest(`/api/v1/pipelines/${encodeURIComponent(pipelineKey)}/boxes`, {
    limit: 5000,
    page: 0,
  })
  return toEntityArray(payload, ['boxes'])
}

function toFlatCellValue(value) {
  if (value === undefined || value === null) {
    return ''
  }

  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
    return value
  }

  if (Array.isArray(value)) {
    return value
      .map(toFlatCellValue)
      .map((item) => String(item).trim())
      .filter(Boolean)
      .join(' | ')
  }

  if (typeof value === 'object') {
    if ('displayName' in value && value.displayName) {
      return String(value.displayName)
    }

    if ('label' in value && value.label) {
      return String(value.label)
    }

    if ('name' in value && value.name) {
      return String(value.name)
    }

    if ('value' in value) {
      return toFlatCellValue(value.value)
    }

    if ('email' in value && value.email) {
      return String(value.email)
    }

    if ('timestamp' in value && Number.isFinite(Number(value.timestamp))) {
      return new Date(Number(value.timestamp)).toISOString()
    }
  }

  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function formatUnixMillisAsDayKey(timestamp) {
  const numericTimestamp = Number(timestamp)
  if (!Number.isFinite(numericTimestamp) || numericTimestamp <= 0) {
    return ''
  }

  const date = new Date(numericTimestamp)
  if (Number.isNaN(date.getTime())) {
    return ''
  }

  const year = date.getUTCFullYear()
  const month = String(date.getUTCMonth() + 1).padStart(2, '0')
  const day = String(date.getUTCDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function decodeStreakFieldOptionValue(optionByKey, rawValue) {
  const rawText = String(rawValue ?? '').trim()
  if (!rawText) {
    return ''
  }

  return optionByKey[rawText] ?? rawText
}

function decodeStreakFieldValue(fieldMeta, rawValue) {
  if (!fieldMeta) {
    return toFlatCellValue(rawValue)
  }

  if (fieldMeta.type === 'DATE') {
    if (Array.isArray(rawValue)) {
      return rawValue
        .map((value) => formatUnixMillisAsDayKey(value) || toFlatCellValue(value))
        .map((value) => String(value).trim())
        .filter(Boolean)
        .join(' | ')
    }

    return formatUnixMillisAsDayKey(rawValue) || toFlatCellValue(rawValue)
  }

  if (fieldMeta.type === 'DROPDOWN' || fieldMeta.type === 'TAG') {
    if (Array.isArray(rawValue)) {
      return rawValue
        .map((value) => decodeStreakFieldOptionValue(fieldMeta.optionByKey, value))
        .map((value) => String(value).trim())
        .filter(Boolean)
        .join(' | ')
    }

    return decodeStreakFieldOptionValue(fieldMeta.optionByKey, rawValue)
  }

  return toFlatCellValue(rawValue)
}

function extractBoxFieldEntries(box) {
  const candidates = [box?.fields, box?.fieldValues, box?.customFields]
  const entries = []

  candidates.forEach((candidate) => {
    if (!candidate) {
      return
    }

    if (Array.isArray(candidate)) {
      candidate.forEach((fieldValue) => {
        if (!fieldValue || typeof fieldValue !== 'object') {
          return
        }

        const fieldKey = String(fieldValue.fieldKey ?? fieldValue.key ?? '')
        if (!fieldKey) {
          return
        }

        const rawValue =
          fieldValue.value ?? fieldValue.fieldValue ?? fieldValue.displayValue ?? fieldValue
        entries.push([fieldKey, rawValue])
      })
      return
    }

    if (typeof candidate === 'object') {
      Object.entries(candidate).forEach(([fieldKey, rawValue]) => {
        entries.push([String(fieldKey), rawValue])
      })
    }
  })

  return entries
}

function mapStreakBoxesToRows(boxes, stages, fields) {
  const stageNameByKey = new Map(stages.map((stage) => [stage.key, stage.name]))
  const fieldMetaByKey = new Map(fields.map((field) => [field.key, field]))

  return boxes
    .map((box) => {
      const boxKey = String(box?.boxKey ?? box?.key ?? '')
      const stageKey = String(box?.stageKey ?? box?.stage?.stageKey ?? box?.stage?.key ?? '')
      const stageNameFromBox = String(box?.stage?.name ?? box?.stageName ?? '')
      const row = {
        Name: String(box?.name ?? ''),
        Etapa: stageNameFromBox || stageNameByKey.get(stageKey) || stageKey,
        'Box Key': boxKey,
      }

      extractBoxFieldEntries(box).forEach(([fieldKey, fieldValue]) => {
        const fieldMeta = fieldMetaByKey.get(fieldKey)
        const fieldName = fieldMeta?.name ?? fieldKey
        row[fieldName] = decodeStreakFieldValue(fieldMeta, fieldValue)
      })

      return row
    })
    .filter((row) => Object.values(row).some((value) => String(value ?? '').trim()))
}

const app = express()
app.use(cors())
app.use(express.json({ limit: '25mb' }))

app.get('/api/health', (_request, response) => {
  response.json({
    ok: true,
    storage: storageLabel,
    databasePath,
    now: new Date().toISOString(),
  })
})

app.get('/api/daily-history', async (_request, response) => {
  response.json({ snapshots: await readModelSnapshots() })
})

app.put('/api/daily-history/:dayKey', async (request, response) => {
  const dayKey = String(request.params.dayKey ?? '')
  if (!isValidDayKey(dayKey)) {
    response.status(400).json({ error: 'El parametro dayKey debe tener formato YYYY-MM-DD valido.' })
    return
  }

  const snapshot = normalizeSnapshotPayload(dayKey, request.body)
  await dbUpsertSnapshot(snapshot)
  const dashboardPayload = normalizeDashboardPayload(dayKey, request.body?.payload, snapshot)
  if (dashboardPayload) {
    await dbUpsertPayload(dashboardPayload)
  }

  response.json({ snapshots: await readModelSnapshots() })
})

app.get('/api/daily-history/:dayKey/payload', async (request, response) => {
  const dayKey = String(request.params.dayKey ?? '')
  if (!isValidDayKey(dayKey)) {
    response.status(400).json({ error: 'El parametro dayKey debe tener formato YYYY-MM-DD valido.' })
    return
  }

  const payload = await readDashboardPayload(dayKey)
  if (!payload) {
    response.status(404).json({
      error:
        'No hay dashboard completo guardado para esta fecha. Guarda nuevamente ese dia para habilitar carga completa.',
    })
    return
  }

  response.json(payload)
})

app.delete('/api/daily-history', async (_request, response) => {
  await dbDeleteSnapshots()
  await dbDeletePayloads()
  response.status(204).send()
})

app.get('/api/streak/status', (_request, response) => {
  response.json({
    configured: Boolean(getStreakApiKey()),
    baseUrl: STREAK_API_BASE_URL,
  })
})

app.get('/api/streak/pipelines', async (_request, response) => {
  if (!getStreakApiKey()) {
    response.json({
      configured: false,
      pipelines: [],
      message: 'Configura STREAK_API_KEY en el backend para conectar con Streak.',
    })
    return
  }

  try {
    const pipelines = await fetchStreakPipelines()
    response.json({
      configured: true,
      pipelines,
    })
  } catch (error) {
    response.status(error.statusCode ?? 500).json({
      error: error.message ?? 'No se pudieron cargar los pipelines de Streak.',
    })
  }
})

app.get('/api/streak/pipelines/:pipelineKey/boxes', async (request, response) => {
  const pipelineKey = String(request.params.pipelineKey ?? '').trim()
  if (!pipelineKey) {
    response.status(400).json({ error: 'Debes enviar un pipelineKey valido.' })
    return
  }

  try {
    const [pipeline, stages, fields, boxes] = await Promise.all([
      fetchStreakPipeline(pipelineKey),
      fetchStreakStages(pipelineKey),
      fetchStreakFields(pipelineKey),
      fetchStreakBoxes(pipelineKey),
    ])

    const rows = mapStreakBoxesToRows(boxes, stages, fields)
    const headerSet = new Set(['Name', 'Etapa', 'Box Key'])
    rows.forEach((row) => {
      Object.keys(row).forEach((header) => headerSet.add(header))
    })

    response.json({
      pipeline,
      headers: Array.from(headerSet),
      rows,
      totalBoxes: rows.length,
      fetchedAt: new Date().toISOString(),
    })
  } catch (error) {
    response.status(error.statusCode ?? 500).json({
      error: error.message ?? 'No se pudieron cargar las cajas desde Streak.',
    })
  }
})

app.use((error, _request, response, next) => {
  void next
  console.error(error)
  response.status(500).json({ error: 'Error interno del servidor.' })
})

const server = app.listen(PORT, () => {
  console.log(`[dashboard-api] Listening on http://localhost:${PORT}`)
  console.log(`[dashboard-api] Storage: ${storageLabel}`)
})

async function closeStorage() {
  if (usePostgres) {
    await pgPool.end()
    return
  }

  if (database) {
    database.close()
  }
}

function shutdown(signal) {
  console.log(`[dashboard-api] Shutting down (${signal})`)
  server.close(async () => {
    await closeStorage()
    process.exit(0)
  })
}

process.on('SIGINT', () => shutdown('SIGINT'))
process.on('SIGTERM', () => shutdown('SIGTERM'))
