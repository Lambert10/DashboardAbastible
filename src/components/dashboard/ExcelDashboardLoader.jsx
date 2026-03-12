import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ExecutiveDashboard from './ExecutiveDashboard'
import GroupContactTable from './GroupContactTable'
import RescuedAnalysisCard from './RescuedAnalysisCard'
import CitationAnalysisCard from './CitationAnalysisCard'
import EvolutionHistoryCard from './EvolutionHistoryCard'
import './ExcelDashboardLoader.css'

const COLOR_PALETTE = ['#2563eb', '#0ea5e9', '#14b8a6', '#22c55e', '#f59e0b', '#ef4444']
const API_BASE_URL = String(import.meta.env.VITE_API_BASE_URL ?? '')
  .trim()
  .replace(/\/+$/, '')
const STREAK_API_FALLBACK_BASE_URL = 'https://dashboardabastible.onrender.com'
const DAILY_HISTORY_API_FALLBACK_BASE_URL = 'https://dashboardabastible.onrender.com'
const DAILY_HISTORY_API_PATH = `${API_BASE_URL}/api/daily-history`
const DAILY_HISTORY_API_FALLBACK_PATH = `${DAILY_HISTORY_API_FALLBACK_BASE_URL}/api/daily-history`
const STREAK_PIPELINES_API_PATH = `${API_BASE_URL}/api/streak/pipelines`
const MAX_DAILY_HISTORY_RECORDS = 730
const LEGACY_SEGMENT_END_DAY_KEY = '2026-02-25'
const OFFICIAL_SEGMENT_START_DAY_KEY = '2026-03-04'
const OFFICIAL_TARGET_TOTAL_PROVIDERS = 1420
const OFFICIAL_TARGET_TOTAL_TOLERANCE = 1
const LEGACY_MIN_TOTAL_PROVIDERS = 3000
const KNOWN_OFFICIAL_TOTAL_ANOMALY_BY_DAY = {
  '2026-03-06': 1419,
}

const INITIAL_MAPPING = {
  group: '',
  stage: '',
  enrolled: '',
  citationDay: '',
  trainingDay: '',
  name: '',
  providerId: '',
  rescuedBy: '',
  pilot: '',
}

const FIELD_LABELS = {
  group: 'Grupo',
  stage: 'Etapa',
  enrolled: 'Enrolado',
  citationDay: 'Dia de Citacion',
  trainingDay: 'Dia de Capacitacion',
  name: 'Name',
  providerId: 'ID Proveedor',
  rescuedBy: 'Rescatado por',
  pilot: 'Piloto',
}

const FIELD_ALIASES = {
  group: ['grupo', 'group'],
  stage: ['stage', 'etapa', 'estado del contacto', 'estado'],
  enrolled: ['enrolado', 'enrolled'],
  citationDay: ['dia de citacion', 'dia citacion', 'fecha de citacion'],
  trainingDay: ['dia de capacitacion', 'dia capacitacion', 'fecha de capacitacion'],
  name: ['name', 'nombre'],
  providerId: ['id proveedor', 'id_provider', 'provider id'],
  rescuedBy: ['rescatado por', 'rescado por', 'rescate by'],
  pilot: ['piloto', 'pilot'],
}

function normalizeText(value) {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
}

function hasValue(value) {
  const text = String(value ?? '').trim()
  if (!text) {
    return false
  }

  const normalized = normalizeText(text)
  return normalized !== 'null' && normalized !== 'nan'
}

function findHeaderByAliases(headers, aliases) {
  const normalizedHeaders = headers.map((header) => ({
    original: header,
    normalized: normalizeText(header),
  }))

  for (const alias of aliases) {
    const normalizedAlias = normalizeText(alias)
    const exactMatch = normalizedHeaders.find((header) => header.normalized === normalizedAlias)
    if (exactMatch) {
      return exactMatch.original
    }

    const partialMatch = normalizedHeaders.find((header) =>
      header.normalized.includes(normalizedAlias),
    )
    if (partialMatch) {
      return partialMatch.original
    }
  }

  return ''
}

function inferMapping(headers) {
  return {
    group: findHeaderByAliases(headers, FIELD_ALIASES.group),
    stage: findHeaderByAliases(headers, FIELD_ALIASES.stage),
    enrolled: findHeaderByAliases(headers, FIELD_ALIASES.enrolled),
    citationDay: findHeaderByAliases(headers, FIELD_ALIASES.citationDay),
    trainingDay: findHeaderByAliases(headers, FIELD_ALIASES.trainingDay),
    name: findHeaderByAliases(headers, FIELD_ALIASES.name),
    providerId: findHeaderByAliases(headers, FIELD_ALIASES.providerId),
    rescuedBy: findHeaderByAliases(headers, FIELD_ALIASES.rescuedBy),
    pilot: findHeaderByAliases(headers, FIELD_ALIASES.pilot),
  }
}

function resolveDefaultStage(stageValues, keywords) {
  if (!stageValues.length) {
    return ''
  }

  const found = stageValues.find((stage) => {
    const normalizedStage = normalizeText(stage)
    return keywords.some((keyword) => normalizedStage.includes(keyword))
  })

  return found || stageValues[0]
}

function getHeaderDisplayName(header) {
  return normalizeText(header) === 'stage' ? 'Etapa' : header
}

function isPilotFlag(value) {
  if (!hasValue(value)) {
    return false
  }

  const normalized = normalizeText(value)
  if (['no', 'false', '0', 'n'].includes(normalized)) {
    return false
  }

  return true
}

function normalizeGroupDisplayLabel(value) {
  return String(value ?? '')
    .replace(/\bprimer\b/gi, '1er')
    .replace(/\bsegundo\b/gi, '2do')
}

function extractGroupOrder(value) {
  const normalized = normalizeText(value)

  if (normalized.includes('primer grupo')) {
    return 1
  }

  if (normalized.includes('segundo grupo')) {
    return 2
  }

  const ordinalMatch = normalized.match(/\b(\d+)\s*(?:er|do|to|mo|vo|no)?\b/)
  if (ordinalMatch) {
    return Number(ordinalMatch[1])
  }

  return Number.POSITIVE_INFINITY
}

function compareGroupLabelsAsc(firstLabel, secondLabel) {
  const firstOrder = extractGroupOrder(firstLabel)
  const secondOrder = extractGroupOrder(secondLabel)

  if (firstOrder !== secondOrder) {
    return firstOrder - secondOrder
  }

  return String(firstLabel).localeCompare(String(secondLabel), 'es')
}

const PROJECT_EXCLUDED_GROUP_KEYWORDS = [
  'sin contactar',
  'no contactar',
  'pendiente de contactar',
  'fuera de alcance',
  'descart',
  'eliminad',
]

function inferProjectGroupNumber(rawGroupLabel) {
  const normalized = normalizeText(rawGroupLabel)
  const explicitPatterns = [
    { pattern: /\bprimer\s+grupo\b|\b1er\s+grupo\b|\bgrupo\s+1\b/, number: 1 },
    { pattern: /\bsegundo\s+grupo\b|\b2do\s+grupo\b|\bgrupo\s+2\b/, number: 2 },
    { pattern: /\btercer\s+grupo\b|\b3er\s+grupo\b|\bgrupo\s+3\b/, number: 3 },
    { pattern: /\bcuarto\s+grupo\b|\b4to\s+grupo\b|\bgrupo\s+4\b/, number: 4 },
    { pattern: /\bquinto\s+grupo\b|\b5to\s+grupo\b|\bgrupo\s+5\b/, number: 5 },
    { pattern: /\bsexto\s+grupo\b|\b6to\s+grupo\b|\bgrupo\s+6\b/, number: 6 },
    { pattern: /\bseptimo\s+grupo\b|\b7mo\s+grupo\b|\bgrupo\s+7\b/, number: 7 },
    { pattern: /\boctavo\s+grupo\b|\b8vo\s+grupo\b|\bgrupo\s+8\b/, number: 8 },
    { pattern: /\bnoveno\s+grupo\b|\b9no\s+grupo\b|\bgrupo\s+9\b/, number: 9 },
  ]

  const explicitMatch = explicitPatterns.find(({ pattern }) => pattern.test(normalized))
  if (explicitMatch) {
    return explicitMatch.number
  }

  const numericMatch =
    normalized.match(/\b([1-9])\s*(?:er|do|to|mo|vo|no)?\s+grupo\b/) ??
    normalized.match(/\bgrupo\s+([1-9])\b/)
  if (!numericMatch) {
    return null
  }

  const parsed = Number(numericMatch[1])
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 9) {
    return null
  }

  return parsed
}

function resolveProjectScopeForRow(row, mapping) {
  if (!mapping.group) {
    return { included: true, groupLabel: 'Sin grupo' }
  }

  const rawGroupValue = hasValue(row[mapping.group]) ? String(row[mapping.group]).trim() : ''
  const normalizedGroup = normalizeText(rawGroupValue)

  if (!rawGroupValue || normalizedGroup === 'sin grupo' || normalizedGroup.includes('sin grupo')) {
    return { included: true, groupLabel: 'Sin grupo' }
  }

  if (PROJECT_EXCLUDED_GROUP_KEYWORDS.some((keyword) => normalizedGroup.includes(keyword))) {
    return { included: false, groupLabel: '' }
  }

  const projectGroupNumber = inferProjectGroupNumber(rawGroupValue)
  if (projectGroupNumber) {
    return { included: true, groupLabel: normalizeGroupDisplayLabel(rawGroupValue) }
  }

  return { included: false, groupLabel: '' }
}

function pad2(value) {
  return String(value).padStart(2, '0')
}

function buildDayKey(year, month, day) {
  return `${year}-${pad2(month)}-${pad2(day)}`
}

function formatDayLabel(dayKey) {
  const [year, month, day] = String(dayKey).split('-')
  if (!year || !month || !day) {
    return String(dayKey)
  }

  return `${day}/${month}/${year}`
}

function parseDayKeyParts(dayKey, fallbackDate = new Date()) {
  if (isValidDayKey(dayKey)) {
    const [yearRaw, monthRaw] = String(dayKey).split('-')
    return {
      year: Number(yearRaw),
      month: Number(monthRaw),
    }
  }

  return {
    year: fallbackDate.getFullYear(),
    month: fallbackDate.getMonth() + 1,
  }
}

function shiftCalendarMonthCursor(cursor, monthOffset) {
  const baseYear = Number(cursor?.year)
  const baseMonth = Number(cursor?.month)
  if (!Number.isInteger(baseYear) || !Number.isInteger(baseMonth)) {
    const now = new Date()
    return { year: now.getFullYear(), month: now.getMonth() + 1 }
  }

  const shifted = new Date(Date.UTC(baseYear, baseMonth - 1 + monthOffset, 1))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
  }
}

function buildMiniCalendarCells(year, month) {
  if (!Number.isInteger(year) || !Number.isInteger(month)) {
    return []
  }

  const firstDayDate = new Date(Date.UTC(year, month - 1, 1))
  const firstWeekdayMondayBased = (firstDayDate.getUTCDay() + 6) % 7
  const startDate = new Date(Date.UTC(year, month - 1, 1 - firstWeekdayMondayBased))

  return Array.from({ length: 42 }, (_, index) => {
    const date = new Date(startDate)
    date.setUTCDate(startDate.getUTCDate() + index)
    const cellYear = date.getUTCFullYear()
    const cellMonth = date.getUTCMonth() + 1
    const cellDay = date.getUTCDate()
    return {
      dayKey: buildDayKey(cellYear, cellMonth, cellDay),
      dayNumber: cellDay,
      inCurrentMonth: cellYear === year && cellMonth === month,
    }
  })
}

const MONTH_NAME_TO_NUMBER = {
  jan: 1,
  january: 1,
  ene: 1,
  enero: 1,
  feb: 2,
  february: 2,
  febrero: 2,
  mar: 3,
  march: 3,
  marzo: 3,
  apr: 4,
  april: 4,
  abr: 4,
  abril: 4,
  may: 5,
  mayo: 5,
  jun: 6,
  june: 6,
  junio: 6,
  jul: 7,
  july: 7,
  julio: 7,
  aug: 8,
  august: 8,
  ago: 8,
  agosto: 8,
  sep: 9,
  sept: 9,
  september: 9,
  septiembre: 9,
  oct: 10,
  october: 10,
  octubre: 10,
  nov: 11,
  november: 11,
  noviembre: 11,
  dec: 12,
  december: 12,
  dic: 12,
  diciembre: 12,
}

const WEEKDAY_NAMES = new Set([
  'lunes',
  'martes',
  'miercoles',
  'jueves',
  'viernes',
  'sabado',
  'domingo',
  'monday',
  'tuesday',
  'wednesday',
  'thursday',
  'friday',
  'saturday',
  'sunday',
])

function toFourDigitYear(rawYear, fallbackYear = null) {
  if (rawYear === undefined || rawYear === null || rawYear === '') {
    return Number.isInteger(fallbackYear) ? fallbackYear : null
  }

  const numericYear = Number(rawYear)
  if (!Number.isInteger(numericYear)) {
    return null
  }

  return numericYear < 100 ? numericYear + 2000 : numericYear
}

function toValidDayKey(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return ''
  }

  if (year < 1900 || month < 1 || month > 12 || day < 1 || day > 31) {
    return ''
  }

  const date = new Date(Date.UTC(year, month - 1, day))
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() + 1 !== month ||
    date.getUTCDate() !== day
  ) {
    return ''
  }

  return buildDayKey(year, month, day)
}

function parseExcelSerialDayKey(serial) {
  if (!Number.isFinite(serial)) {
    return ''
  }

  const excelEpochUtcMs = Date.UTC(1899, 11, 30)
  const integerDays = Math.floor(serial)
  if (integerDays < 30000) {
    return ''
  }
  const fromExcel = new Date(excelEpochUtcMs + integerDays * 86400000)
  if (Number.isNaN(fromExcel.getTime())) {
    return ''
  }

  return toValidDayKey(
    fromExcel.getUTCFullYear(),
    fromExcel.getUTCMonth() + 1,
    fromExcel.getUTCDate(),
  )
}

function parseUnixTimestampDayKey(value) {
  if (!Number.isFinite(value)) {
    return ''
  }

  let timestampMs = value
  if (Math.abs(timestampMs) >= 1e9 && Math.abs(timestampMs) < 1e11) {
    timestampMs *= 1000
  }

  if (Math.abs(timestampMs) < 1e11) {
    return ''
  }

  const date = new Date(timestampMs)
  if (Number.isNaN(date.getTime())) {
    return ''
  }

  return toValidDayKey(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate())
}

function parseDayKey(value, defaultYear = null) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return toValidDayKey(value.getUTCFullYear(), value.getUTCMonth() + 1, value.getUTCDate())
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    const fromUnix = parseUnixTimestampDayKey(value)
    if (fromUnix) {
      return fromUnix
    }

    return parseExcelSerialDayKey(value)
  }

  const raw = String(value ?? '').trim()
  if (!raw) {
    return ''
  }

  if (/^\d+(?:[.,]\d+)?$/.test(raw)) {
    const numericRaw = Number(raw.replace(',', '.'))
    const fromUnix = parseUnixTimestampDayKey(numericRaw)
    if (fromUnix) {
      return fromUnix
    }

    const fromSerial = parseExcelSerialDayKey(numericRaw)
    if (fromSerial) {
      return fromSerial
    }
  }

  const withoutIsoTime = raw.split('T')[0].trim()
  const withoutTrailingTime = withoutIsoTime
    .replace(/\s+\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?(?:\s*[ap]\.?m\.?)?$/i, '')
    .trim()
  const onlyDate = withoutTrailingTime.includes(',')
    ? withoutTrailingTime.split(',').pop().trim()
    : withoutTrailingTime

  let match = onlyDate.match(/^(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})$/)
  if (match) {
    const year = Number(match[1])
    const month = Number(match[2])
    const day = Number(match[3])
    const parsed = toValidDayKey(year, month, day)
    if (parsed) {
      return parsed
    }
  }

  match = onlyDate.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})$/)
  if (match) {
    const day = Number(match[1])
    const month = Number(match[2])
    const year = toFourDigitYear(match[3], defaultYear)
    const parsed = toValidDayKey(year, month, day)
    if (parsed) {
      return parsed
    }
  }

  const normalizedDate = normalizeText(onlyDate)
  match = normalizedDate.match(/^(\d{1,2})\s+([a-z]+)\s+(\d{2,4})$/)
  if (match) {
    const day = Number(match[1])
    const month = MONTH_NAME_TO_NUMBER[match[2]]
    const year = toFourDigitYear(match[3], defaultYear)
    const parsed = toValidDayKey(year, month, day)
    if (parsed) {
      return parsed
    }
  }

  match = normalizedDate.match(/^([a-z]+)\s+(\d{1,2})\s+(\d{2,4})$/)
  if (match) {
    const month = MONTH_NAME_TO_NUMBER[match[1]]
    const day = Number(match[2])
    const year = toFourDigitYear(match[3], defaultYear)
    const parsed = toValidDayKey(year, month, day)
    if (parsed) {
      return parsed
    }
  }

  match = normalizedDate.match(/^(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+(\d{2,4}))?$/)
  if (match) {
    const day = Number(match[1])
    const month = MONTH_NAME_TO_NUMBER[match[2]]
    const year = toFourDigitYear(match[3], defaultYear)
    const parsed = toValidDayKey(year, month, day)
    if (parsed) {
      return parsed
    }
  }

  match = normalizedDate.match(/^([a-z]+)\s+(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+(\d{2,4}))?$/)
  if (match) {
    const weekday = match[1]
    const day = Number(match[2])
    const month = MONTH_NAME_TO_NUMBER[match[3]]
    const year = toFourDigitYear(match[4], defaultYear)
    if (WEEKDAY_NAMES.has(weekday)) {
      const parsed = toValidDayKey(year, month, day)
      if (parsed) {
        return parsed
      }
    }
  }

  const fallback = new Date(raw)
  if (!Number.isNaN(fallback.getTime())) {
    return toValidDayKey(
      fallback.getUTCFullYear(),
      fallback.getUTCMonth() + 1,
      fallback.getUTCDate(),
    )
  }

  return ''
}

function sanitizeTrainingDayLabel(value) {
  return String(value ?? '').replace(/\s+/g, ' ').trim()
}

function hasExplicitYearToken(value) {
  const raw = String(value ?? '').trim()
  if (!raw) {
    return false
  }

  const normalized = normalizeText(raw)
  return (
    /\b\d{4}\b/.test(raw) ||
    /\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b/.test(raw) ||
    /\b\d{1,2}\s+[a-z]+\s+\d{2,4}\b/.test(normalized) ||
    /\b[a-z]+\s+\d{1,2}\s+\d{2,4}\b/.test(normalized) ||
    /\b\d{1,2}\s+de\s+[a-z]+(?:\s+de\s+\d{2,4})\b/.test(normalized) ||
    /\b[a-z]+\s+\d{1,2}\s+de\s+[a-z]+(?:\s+de\s+\d{2,4})\b/.test(normalized)
  )
}

function parseTrainingDayBuckets(value, defaultYear = null) {
  const directDayKey = parseDayKey(value, defaultYear)
  const inferredYear = !hasExplicitYearToken(value)
  if (directDayKey) {
    return [
      {
        bucketKey: `0:${directDayKey}`,
        label: formatDayLabel(directDayKey),
        inferredYear,
      },
    ]
  }

  const raw = sanitizeTrainingDayLabel(value)
  if (!raw) {
    return []
  }

  const buckets = []
  const seen = new Set()
  const addParsedDay = (dayKey) => {
    if (!dayKey) {
      return
    }

    const bucketKey = `0:${dayKey}`
    if (seen.has(bucketKey)) {
      return
    }

    seen.add(bucketKey)
    buckets.push({ bucketKey, label: formatDayLabel(dayKey), inferredYear })
  }

  const tokenPatterns = [
    /\b\d{4}[/.-]\d{1,2}[/.-]\d{1,2}\b/g,
    /\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b/g,
    /\b\d{1,2}\s+[A-Za-z\u00c0-\u017f]+\.?\s+\d{2,4}\b/gi,
    /\b[A-Za-z\u00c0-\u017f]+\.?\s+\d{1,2}\s+\d{2,4}\b/gi,
    /\b(?:[A-Za-z\u00c0-\u017f]+\s+)?\d{1,2}\s+de\s+[A-Za-z\u00c0-\u017f]+(?:\s+de\s+\d{2,4})?\b/gi,
  ]

  tokenPatterns.forEach((pattern) => {
    const matches = raw.match(pattern) ?? []
    matches.forEach((match) => {
      addParsedDay(parseDayKey(match, defaultYear))
    })
  })

  if (buckets.length) {
    return buckets
  }
  return []
}

function inferTrainingYear(rows, mapping) {
  if (!mapping.trainingDay) {
    return new Date().getUTCFullYear()
  }

  const yearCounts = new Map()

  rows.forEach((row) => {
    if (!hasValue(row[mapping.trainingDay])) {
      return
    }

    const buckets = parseTrainingDayBuckets(row[mapping.trainingDay])
    buckets.forEach(({ bucketKey }) => {
      const dayKey = bucketKey.startsWith('0:') ? bucketKey.slice(2) : ''
      const year = Number(dayKey.split('-')[0])
      if (!Number.isInteger(year)) {
        return
      }

      yearCounts.set(year, (yearCounts.get(year) ?? 0) + 1)
    })
  })

  if (!yearCounts.size) {
    return new Date().getUTCFullYear()
  }

  return Array.from(yearCounts.entries()).sort((a, b) => b[1] - a[1] || b[0] - a[0])[0][0]
}

function shiftDayKeyYear(dayKey, yearOffset) {
  const [yearRaw, monthRaw, dayRaw] = String(dayKey).split('-')
  const year = Number(yearRaw)
  const month = Number(monthRaw)
  const day = Number(dayRaw)
  return toValidDayKey(year + yearOffset, month, day)
}

function applyRolloverToInferredDays(trainingDayMap, inferredYear) {
  const allBuckets = Array.from(trainingDayMap.entries()).map(([bucketKey, bucket]) => {
    const dayKey = bucketKey.startsWith('0:') ? bucketKey.slice(2) : ''
    const [, monthRaw] = dayKey.split('-')
    const month = Number(monthRaw)
    return { bucketKey, bucket, dayKey, month }
  })

  const hasEarlyMonths = allBuckets.some(({ month }) => month >= 1 && month <= 3)
  const hasLateInferredMonths = allBuckets.some(
    ({ month, dayKey, bucket }) =>
      bucket.inferredYear &&
      month >= 10 &&
      month <= 12 &&
      Number(dayKey.split('-')[0]) === inferredYear,
  )
  const hasMidMonths = allBuckets.some(({ month }) => month >= 4 && month <= 9)

  if (!hasEarlyMonths || !hasLateInferredMonths || hasMidMonths) {
    return
  }

  allBuckets.forEach(({ bucketKey, bucket, dayKey, month }) => {
    if (!(bucket.inferredYear && month >= 10 && month <= 12)) {
      return
    }

    const year = Number(dayKey.split('-')[0])
    if (year !== inferredYear) {
      return
    }

    const shiftedDayKey = shiftDayKeyYear(dayKey, -1)
    if (!shiftedDayKey) {
      return
    }

    const shiftedBucketKey = `0:${shiftedDayKey}`
    if (!trainingDayMap.has(shiftedBucketKey)) {
      trainingDayMap.set(shiftedBucketKey, {
        label: formatDayLabel(shiftedDayKey),
        providers: new Set(),
        inferredYear: true,
      })
    }

    const shiftedBucket = trainingDayMap.get(shiftedBucketKey)
    bucket.providers.forEach((providerId) => shiftedBucket.providers.add(providerId))
    trainingDayMap.delete(bucketKey)
  })
}

function getTrainingBucketSortKey(bucketKey) {
  if (String(bucketKey).startsWith('0:')) {
    return String(bucketKey).slice(2)
  }

  return `9999-99-99:${bucketKey}`
}

function getLocalDayKey(date = new Date()) {
  return buildDayKey(date.getFullYear(), date.getMonth() + 1, date.getDate())
}

function toSafeNumber(value) {
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : 0
}

function isValidDayKey(value) {
  return typeof value === 'string' && parseDayKey(value) === value
}

function classifySnapshotTimeline(snapshot) {
  if (!snapshot) {
    return { segment: 'unknown', reason: 'Snapshot no disponible.' }
  }

  const dayKey = String(snapshot.dayKey ?? '')
  const totalProviders = Math.max(0, Math.round(toSafeNumber(snapshot.totalProviders)))
  const knownAnomalyTotal = KNOWN_OFFICIAL_TOTAL_ANOMALY_BY_DAY[dayKey]
  if (Number.isFinite(knownAnomalyTotal) && totalProviders === knownAnomalyTotal) {
    return {
      segment: 'official',
      reason: `Anomalia conocida (${totalProviders}) aceptada para ${formatDayLabel(dayKey)}.`,
    }
  }

  if (totalProviders >= LEGACY_MIN_TOTAL_PROVIDERS) {
    return {
      segment: 'legacy',
      reason: `Universo >= ${LEGACY_MIN_TOTAL_PROVIDERS} proveedores.`,
    }
  }

  if (Math.abs(totalProviders - OFFICIAL_TARGET_TOTAL_PROVIDERS) <= OFFICIAL_TARGET_TOTAL_TOLERANCE) {
    return {
      segment: 'official',
      reason: `Universo objetivo ${OFFICIAL_TARGET_TOTAL_PROVIDERS} +/- ${OFFICIAL_TARGET_TOTAL_TOLERANCE}.`,
    }
  }

  if (isValidDayKey(dayKey) && dayKey <= LEGACY_SEGMENT_END_DAY_KEY) {
    return {
      segment: 'legacy',
      reason: `Fecha <= fin tramo legacy (${formatDayLabel(LEGACY_SEGMENT_END_DAY_KEY)}).`,
    }
  }

  if (isValidDayKey(dayKey) && dayKey >= OFFICIAL_SEGMENT_START_DAY_KEY) {
    return {
      segment: 'official',
      reason: `Fecha >= inicio tramo oficial (${formatDayLabel(OFFICIAL_SEGMENT_START_DAY_KEY)}).`,
    }
  }

  const midpoint = Math.round((OFFICIAL_TARGET_TOTAL_PROVIDERS + LEGACY_MIN_TOTAL_PROVIDERS) / 2)
  if (totalProviders >= midpoint) {
    return {
      segment: 'legacy',
      reason: `Periodo intermedio: total ${totalProviders} mas cercano al tramo legacy.`,
    }
  }

  return {
    segment: 'official',
    reason: `Periodo intermedio: total ${totalProviders} mas cercano al tramo oficial.`,
  }
}

function buildTimelineExplanation(timelineDecision, snapshotCandidate) {
  if (!snapshotCandidate) {
    return ''
  }

  const total = Math.max(0, Math.round(toSafeNumber(snapshotCandidate.totalProviders)))
  if (timelineDecision?.segment === 'legacy') {
    return `Este corte se tratara como historico LEGACY (${total} proveedores) y no se comparara en continuidad directa con el tramo oficial ${OFFICIAL_TARGET_TOTAL_PROVIDERS}.`
  }

  if (timelineDecision?.segment === 'official') {
    return `Este corte entra al tramo OFICIAL y se comparara con el universo objetivo ${OFFICIAL_TARGET_TOTAL_PROVIDERS} (tolerancia +/- ${OFFICIAL_TARGET_TOTAL_TOLERANCE}).`
  }

  return 'No se pudo determinar el tramo automaticamente.'
}

function isAllowedLegacyOfficialBreak(leftSnapshot, rightSnapshot) {
  if (!leftSnapshot || !rightSnapshot) {
    return false
  }

  const left = classifySnapshotTimeline(leftSnapshot)
  const right = classifySnapshotTimeline(rightSnapshot)
  if (left.segment === right.segment) {
    return false
  }

  const legacyDayKey = left.segment === 'legacy' ? leftSnapshot.dayKey : rightSnapshot.dayKey
  const officialDayKey = left.segment === 'official' ? leftSnapshot.dayKey : rightSnapshot.dayKey

  return (
    isValidDayKey(legacyDayKey) &&
    isValidDayKey(officialDayKey) &&
    legacyDayKey <= LEGACY_SEGMENT_END_DAY_KEY &&
    officialDayKey >= OFFICIAL_SEGMENT_START_DAY_KEY
  )
}

function inferDayKeyFromNumericDateParts(firstRaw, secondRaw, yearRaw) {
  const first = Number(firstRaw)
  const second = Number(secondRaw)
  const year = toFourDigitYear(yearRaw)

  if (!Number.isInteger(first) || !Number.isInteger(second) || !Number.isInteger(year)) {
    return ''
  }

  const monthDayYear = toValidDayKey(year, first, second)
  const dayMonthYear = toValidDayKey(year, second, first)

  if (first > 12 && second <= 12) {
    return dayMonthYear
  }

  if (second > 12 && first <= 12) {
    return monthDayYear
  }

  return monthDayYear || dayMonthYear
}

function inferSnapshotDayKeyFromFileName(fileName, fallbackDayKey = getLocalDayKey()) {
  const baseName = String(fileName ?? '').replace(/\.[^.]+$/, '')
  if (!baseName) {
    return fallbackDayKey
  }

  const yearFirstMatch = baseName.match(/\b(19\d{2}|20\d{2})[/._-](\d{1,2})[/._-](\d{1,2})\b/)
  if (yearFirstMatch) {
    const parsed = toValidDayKey(
      Number(yearFirstMatch[1]),
      Number(yearFirstMatch[2]),
      Number(yearFirstMatch[3]),
    )
    if (parsed) {
      return parsed
    }
  }

  const commonNumericMatch = baseName.match(/\b(\d{1,2})[/._-](\d{1,2})[/._-](\d{2,4})\b/)
  if (commonNumericMatch) {
    const parsed = inferDayKeyFromNumericDateParts(
      commonNumericMatch[1],
      commonNumericMatch[2],
      commonNumericMatch[3],
    )
    if (parsed) {
      return parsed
    }
  }

  return fallbackDayKey
}

function inferSnapshotDayKeyFromFile(file, fallbackDayKey = getLocalDayKey()) {
  const lastModified = Number(file?.lastModified)
  if (Number.isFinite(lastModified) && lastModified > 0) {
    const fromFileMetadata = getLocalDayKey(new Date(lastModified))
    if (isValidDayKey(fromFileMetadata)) {
      return fromFileMetadata
    }
  }

  return inferSnapshotDayKeyFromFileName(file?.name, fallbackDayKey)
}

function resolveProviderIdColumn(headers, mapping) {
  const mapped = String(mapping?.providerId ?? '').trim()
  if (mapped) {
    return mapped
  }

  const headerList = Array.isArray(headers) ? headers.map((header) => String(header)) : []
  return inferMapping(headerList).providerId || ''
}

function extractProviderIdsFromRows(rows, providerIdColumn, mappingForScope = INITIAL_MAPPING) {
  if (!providerIdColumn || !Array.isArray(rows) || !rows.length) {
    return []
  }

  const output = []
  rows.forEach((row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return
    }

    const projectScope = resolveProjectScopeForRow(row, mappingForScope)
    if (!projectScope.included) {
      return
    }

    if (!hasValue(row[providerIdColumn])) {
      return
    }

    const providerId = String(row[providerIdColumn]).trim()
    if (!providerId) {
      return
    }

    output.push(providerId)
  })

  return output
}

function extractProviderUniverseFromPayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return []
  }

  const rows = Array.isArray(payload.rows) ? payload.rows : []
  const headers = Array.isArray(payload.headers) ? payload.headers : []
  const mapping =
    payload.mapping && typeof payload.mapping === 'object' && !Array.isArray(payload.mapping)
      ? { ...INITIAL_MAPPING, ...payload.mapping }
      : INITIAL_MAPPING
  const providerIdColumn = resolveProviderIdColumn(headers, mapping)

  if (!providerIdColumn) {
    return []
  }

  const ids = extractProviderIdsFromRows(rows, providerIdColumn, mapping)
  return Array.from(new Set(ids))
}

function formatDashboardSourceLabel(fileName, snapshotDayKey) {
  const rawFileName = String(fileName ?? '').trim()
  if (!rawFileName) {
    return 'Sin archivo cargado'
  }

  const fallbackDayKey = isValidDayKey(snapshotDayKey) ? snapshotDayKey : getLocalDayKey()
  const inferredDayKey = inferSnapshotDayKeyFromFileName(rawFileName, fallbackDayKey)
  const baseName = rawFileName
    .replace(/\.[^.]+$/, '')
    .replace(/\(\s*[^)]*\)\s*$/, '')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  if (!isValidDayKey(inferredDayKey)) {
    return baseName || rawFileName
  }

  if (!baseName) {
    return formatDayLabel(inferredDayKey)
  }

  return `${baseName} (${formatDayLabel(inferredDayKey)})`
}

function normalizeDailyHistorySnapshot(item) {
  if (!item || typeof item !== 'object' || typeof item.dayKey !== 'string') {
    return null
  }

  const timelineDecision = classifySnapshotTimeline(item)

  return {
    dayKey: String(item.dayKey),
    fileName: String(item.fileName ?? ''),
    sheetName: String(item.sheetName ?? ''),
    totalProviders: toSafeNumber(item.totalProviders),
    contactedProviders: toSafeNumber(item.contactedProviders),
    trainedProviders: toSafeNumber(item.trainedProviders),
    enrolledProviders: toSafeNumber(item.enrolledProviders),
    rescuedProviders: toSafeNumber(item.rescuedProviders),
    citedProviders: toSafeNumber(item.citedProviders),
    trainingDaysCount: toSafeNumber(item.trainingDaysCount),
    contactRate: toSafeNumber(item.contactRate),
    trainedRate: toSafeNumber(item.trainedRate),
    rescueRate: toSafeNumber(item.rescueRate),
    savedAt: String(item.savedAt ?? ''),
    hasPayload: Boolean(item.hasPayload),
    timelineSegment:
      String(item.timelineSegment ?? '').trim() || timelineDecision.segment,
    timelineReason:
      String(item.timelineReason ?? '').trim() || timelineDecision.reason,
  }
}

function normalizeDailyHistorySnapshots(items) {
  if (!Array.isArray(items)) {
    return []
  }

  return items
    .map(normalizeDailyHistorySnapshot)
    .filter(Boolean)
    .sort((a, b) => a.dayKey.localeCompare(b.dayKey))
    .slice(-MAX_DAILY_HISTORY_RECORDS)
}

function normalizeStoredDashboardPayload(item) {
  if (!item || typeof item !== 'object') {
    return null
  }

  const dayKey = String(item.dayKey ?? '')
  if (!isValidDayKey(dayKey)) {
    return null
  }

  const rows = Array.isArray(item.rows)
    ? item.rows.filter((row) => row && typeof row === 'object' && !Array.isArray(row))
    : []
  const headers = Array.isArray(item.headers)
    ? item.headers.map((header) => String(header)).filter(Boolean)
    : []

  const mapping =
    item.mapping && typeof item.mapping === 'object' && !Array.isArray(item.mapping)
      ? {
          ...INITIAL_MAPPING,
          ...Object.fromEntries(
            Object.entries(item.mapping).map(([field, column]) => [String(field), String(column ?? '')]),
          ),
        }
      : INITIAL_MAPPING

  if (!rows.length || !headers.length) {
    return null
  }

  return {
    dayKey,
    fileName: String(item.fileName ?? ''),
    sheetName: String(item.sheetName ?? ''),
    headers,
    rows,
    mapping,
    contactStage: String(item.contactStage ?? ''),
    trainedStage: String(item.trainedStage ?? ''),
    savedAt: String(item.savedAt ?? ''),
  }
}

async function parseApiResponse(response) {
  try {
    return await response.json()
  } catch {
    return {}
  }
}

async function fetchJsonWithFallback(primaryUrl, fallbackUrl, isPayloadValid, fetchOptions = undefined) {
  const request = async (url) => {
    const response = await fetch(url, fetchOptions)
    const payload = await parseApiResponse(response)
    return { response, payload, url }
  }

  const primaryResult = await request(primaryUrl)
  const primaryValid = Boolean(isPayloadValid?.(primaryResult.payload))
  if (primaryResult.response.ok && primaryValid) {
    return primaryResult
  }

  if (!fallbackUrl || fallbackUrl === primaryUrl) {
    return primaryResult
  }

  const fallbackResult = await request(fallbackUrl)
  const fallbackValid = Boolean(isPayloadValid?.(fallbackResult.payload))
  if (fallbackResult.response.ok && fallbackValid) {
    return fallbackResult
  }

  return fallbackResult.response.ok ? primaryResult : fallbackResult
}

function resolveApiErrorMessage(payload, fallbackMessage) {
  if (payload && typeof payload.error === 'string' && payload.error.trim()) {
    return payload.error
  }

  return fallbackMessage
}

async function fetchDailyHistorySnapshots() {
  const { response, payload } = await fetchJsonWithFallback(
    DAILY_HISTORY_API_PATH,
    DAILY_HISTORY_API_FALLBACK_PATH,
    (body) => Array.isArray(body?.snapshots),
  )
  if (!response.ok) {
    throw new Error(resolveApiErrorMessage(payload, 'No se pudo cargar el historial guardado.'))
  }

  return normalizeDailyHistorySnapshots(payload.snapshots)
}

async function saveDailyHistorySnapshot(snapshot, dashboardPayload = null) {
  const requestBody = dashboardPayload ? { ...snapshot, payload: dashboardPayload } : snapshot
  const requestOptions = {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody),
  }
  const { response, payload } = await fetchJsonWithFallback(
    `${DAILY_HISTORY_API_PATH}/${encodeURIComponent(snapshot.dayKey)}`,
    `${DAILY_HISTORY_API_FALLBACK_PATH}/${encodeURIComponent(snapshot.dayKey)}`,
    (body) => Array.isArray(body?.snapshots),
    requestOptions,
  )
  if (!response.ok) {
    throw new Error(resolveApiErrorMessage(payload, 'No se pudo guardar el snapshot en la base de datos.'))
  }

  return normalizeDailyHistorySnapshots(payload.snapshots)
}

async function clearDailyHistorySnapshots() {
  const { response, payload } = await fetchJsonWithFallback(
    DAILY_HISTORY_API_PATH,
    DAILY_HISTORY_API_FALLBACK_PATH,
    () => true,
    { method: 'DELETE' },
  )
  if (!response.ok) {
    throw new Error(resolveApiErrorMessage(payload, 'No se pudo borrar el historial guardado.'))
  }
}

async function fetchDashboardPayloadByDayKey(dayKey) {
  const { response, payload } = await fetchJsonWithFallback(
    `${DAILY_HISTORY_API_PATH}/${encodeURIComponent(dayKey)}/payload`,
    `${DAILY_HISTORY_API_FALLBACK_PATH}/${encodeURIComponent(dayKey)}/payload`,
    (body) => Array.isArray(body?.rows) && Array.isArray(body?.headers),
  )
  if (!response.ok) {
    throw new Error(
      resolveApiErrorMessage(
        payload,
        'No hay dashboard completo guardado para esta fecha. Guarda nuevamente ese dia.',
      ),
    )
  }

  const normalized = normalizeStoredDashboardPayload(payload)
  if (!normalized) {
    throw new Error('El payload guardado para esta fecha es invalido o incompleto.')
  }

  return normalized
}

function validateSnapshotInput(rows, mapping, contactStage, trainedStage, officialProviderIdSet) {
  const errors = []
  const warnings = []

  if (!rows.length) {
    errors.push('No hay filas cargadas para validar.')
    return { errors, warnings, providerCount: 0 }
  }

  if (!mapping.providerId) {
    errors.push('Debes mapear "ID Proveedor" para calcular y guardar el snapshot.')
  }

  if (!mapping.group) {
    errors.push('Debes mapear "Grupo" para mantener la comparabilidad historica.')
  }

  if (!mapping.stage) {
    errors.push('Debes mapear "Etapa" para calcular contactados y capacitados.')
  }

  if (mapping.stage && !contactStage) {
    errors.push('Debes seleccionar la etapa que cuenta como contactado.')
  }

  if (mapping.stage && !trainedStage) {
    errors.push('Debes seleccionar la etapa que cuenta como capacitado.')
  }

  if (mapping.providerId) {
    const scopedRows = rows.filter((row) => resolveProjectScopeForRow(row, mapping).included)
    const providerValues = extractProviderIdsFromRows(rows, mapping.providerId, mapping)
    const uniqueProviders = new Set(providerValues)
    if (!uniqueProviders.size) {
      errors.push('No se detectaron IDs de proveedor validos en las filas cargadas.')
    }

    const providerCoverage = scopedRows.length ? providerValues.length / scopedRows.length : 0
    if (providerCoverage < 0.8) {
      warnings.push(
        `Cobertura baja de ID Proveedor (${(providerCoverage * 100).toFixed(1)}%). Revisa el mapeo antes de guardar.`,
      )
    }

    if (officialProviderIdSet?.size) {
      const matchedProviderIds = Array.from(uniqueProviders).filter((providerId) =>
        officialProviderIdSet.has(providerId),
      )
      const matchedCount = matchedProviderIds.length
      const missingCount = Math.max(officialProviderIdSet.size - matchedCount, 0)
      const outOfUniverseCount = Math.max(uniqueProviders.size - matchedCount, 0)

      if (missingCount > 0) {
        warnings.push(
          `Universo oficial: ${matchedCount}/${officialProviderIdSet.size} IDs encontrados en este corte. Se guardara con total menor (${missingCount} faltantes).`,
        )
      }

      if (outOfUniverseCount > 0) {
        warnings.push(
          `${outOfUniverseCount} IDs del archivo no pertenecen al universo oficial y se excluiran del total.`,
        )
      }

      return { errors, warnings, providerCount: matchedCount }
    }

    return { errors, warnings, providerCount: uniqueProviders.size }
  }

  return { errors, warnings, providerCount: 0 }
}

function validateSnapshotChronology(snapshots, snapshotCandidate) {
  const errors = []
  const warnings = []

  if (!snapshotCandidate) {
    return { errors, warnings }
  }

  const orderedSnapshots = [...snapshots].sort((a, b) => a.dayKey.localeCompare(b.dayKey))
  const previousSnapshot = orderedSnapshots
    .filter((snapshot) => snapshot.dayKey < snapshotCandidate.dayKey)
    .at(-1)
  const nextSnapshot = orderedSnapshots.find((snapshot) => snapshot.dayKey > snapshotCandidate.dayKey)
  const candidateDecision = classifySnapshotTimeline(snapshotCandidate)
  const previousDecision = previousSnapshot ? classifySnapshotTimeline(previousSnapshot) : null
  const nextDecision = nextSnapshot ? classifySnapshotTimeline(nextSnapshot) : null

  const previousIsSameSegment =
    previousSnapshot && previousDecision?.segment === candidateDecision.segment
  const nextIsSameSegment = nextSnapshot && nextDecision?.segment === candidateDecision.segment

  const monotonicFields = [
    { key: 'contactedProviders', label: 'Contactados' },
    { key: 'trainedProviders', label: 'Capacitados' },
    { key: 'rescuedProviders', label: 'Rescatados' },
    { key: 'citedProviders', label: 'Citados' },
  ]

  if (snapshotCandidate.contactedProviders < snapshotCandidate.trainedProviders) {
    errors.push('Inconsistencia: Contactados no puede ser menor que Capacitados.')
  }

  if (snapshotCandidate.totalProviders < snapshotCandidate.contactedProviders) {
    errors.push('Inconsistencia: Proveedores totales no puede ser menor que Contactados.')
  }

  if (snapshotCandidate.totalProviders < snapshotCandidate.trainedProviders) {
    errors.push('Inconsistencia: Proveedores totales no puede ser menor que Capacitados.')
  }

  if (previousSnapshot && previousIsSameSegment) {
    monotonicFields.forEach(({ key, label }) => {
      if (snapshotCandidate[key] < previousSnapshot[key]) {
        errors.push(
          `${label} en ${formatDayLabel(snapshotCandidate.dayKey)} no puede ser menor que ${formatDayLabel(previousSnapshot.dayKey)}.`,
        )
      }
    })
  } else if (previousSnapshot && !isAllowedLegacyOfficialBreak(previousSnapshot, snapshotCandidate)) {
    warnings.push(
      `Cambio de tramo no esperado entre ${formatDayLabel(previousSnapshot.dayKey)} (${previousDecision.segment}) y ${formatDayLabel(snapshotCandidate.dayKey)} (${candidateDecision.segment}).`,
    )
  }

  if (nextSnapshot && nextIsSameSegment) {
    monotonicFields.forEach(({ key, label }) => {
      if (snapshotCandidate[key] > nextSnapshot[key]) {
        errors.push(
          `${label} en ${formatDayLabel(snapshotCandidate.dayKey)} no puede ser mayor que ${formatDayLabel(nextSnapshot.dayKey)}.`,
        )
      }
    })
  } else if (nextSnapshot && !isAllowedLegacyOfficialBreak(snapshotCandidate, nextSnapshot)) {
    warnings.push(
      `Cambio de tramo no esperado entre ${formatDayLabel(snapshotCandidate.dayKey)} (${candidateDecision.segment}) y ${formatDayLabel(nextSnapshot.dayKey)} (${nextDecision.segment}).`,
    )
  }

  const sameDaySnapshot = orderedSnapshots.find((snapshot) => snapshot.dayKey === snapshotCandidate.dayKey)
  if (sameDaySnapshot && sameDaySnapshot.savedAt) {
    warnings.push(
      `Ya existe snapshot para ${formatDayLabel(snapshotCandidate.dayKey)}. Se sobrescribira al guardar.`,
    )
  }

  if (
    candidateDecision.segment === 'official' &&
    snapshotCandidate.totalProviders > OFFICIAL_TARGET_TOTAL_PROVIDERS + OFFICIAL_TARGET_TOTAL_TOLERANCE
  ) {
    warnings.push(
      `Tramo oficial: total ${snapshotCandidate.totalProviders} supera el universo objetivo ${OFFICIAL_TARGET_TOTAL_PROVIDERS} (+${OFFICIAL_TARGET_TOTAL_TOLERANCE} tolerancia).`,
    )
  }

  if (candidateDecision.segment === 'legacy' && snapshotCandidate.dayKey >= OFFICIAL_SEGMENT_START_DAY_KEY) {
    warnings.push(
      `Fecha ${formatDayLabel(snapshotCandidate.dayKey)} cae en periodo oficial pero fue clasificada como legacy.`,
    )
  }

  if (candidateDecision.segment === 'official' && snapshotCandidate.dayKey <= LEGACY_SEGMENT_END_DAY_KEY) {
    warnings.push(
      `Fecha ${formatDayLabel(snapshotCandidate.dayKey)} cae en periodo legacy pero fue clasificada como oficial.`,
    )
  }

  return { errors, warnings }
}

function buildDataQualityProfile({
  rows,
  headers,
  mapping,
  stageValues,
  snapshotDayKey,
  officialProviderIdSet,
}) {
  const mappedColumns = Object.fromEntries(
    Object.entries(mapping).map(([field, column]) => [field, String(column ?? '')]),
  )

  const profile = {
    dayKey: snapshotDayKey,
    totalRows: rows.length,
    scopedRows: 0,
    excludedRowsByScope: 0,
    totalProviders: 0,
    providerCoverageRate: 0,
    duplicateProviders: 0,
    stageValuesCount: stageValues.length,
    numericStageRatio: 0,
    suspiciousDateValues: 0,
    officialUniverseSize: officialProviderIdSet?.size ?? 0,
    officialUniverseMatched: 0,
    officialUniverseMissing: 0,
    headers: [...headers],
    mappedColumns,
  }

  if (!rows.length) {
    return profile
  }

  const scopedRows = rows.filter((row) => resolveProjectScopeForRow(row, mapping).included)
  profile.scopedRows = scopedRows.length
  profile.excludedRowsByScope = Math.max(rows.length - scopedRows.length, 0)

  if (mapping.providerId) {
    const providerIds = extractProviderIdsFromRows(scopedRows, mapping.providerId, mapping)
    const uniqueProviders = new Set(providerIds)
    const filteredProviderIds = officialProviderIdSet?.size
      ? providerIds.filter((providerId) => officialProviderIdSet.has(providerId))
      : providerIds
    const filteredUniqueProviders = new Set(filteredProviderIds)

    profile.totalProviders = filteredUniqueProviders.size
    profile.providerCoverageRate = scopedRows.length ? providerIds.length / scopedRows.length : 0
    profile.duplicateProviders = Math.max(
      filteredProviderIds.length - filteredUniqueProviders.size,
      0,
    )

    if (officialProviderIdSet?.size) {
      profile.officialUniverseMatched = filteredUniqueProviders.size
      profile.officialUniverseMissing = Math.max(
        officialProviderIdSet.size - filteredUniqueProviders.size,
        0,
      )
    } else {
      profile.officialUniverseMatched = uniqueProviders.size
      profile.officialUniverseMissing = 0
    }
  }

  if (mapping.stage) {
    const stageRawValues = scopedRows
      .map((row) => (hasValue(row[mapping.stage]) ? String(row[mapping.stage]).trim() : ''))
      .filter(Boolean)
    const numericStageValues = stageRawValues.filter((value) => /^\d{4,}$/.test(value))
    profile.numericStageRatio = stageRawValues.length
      ? numericStageValues.length / stageRawValues.length
      : 0
  }

  const dateFields = [mapping.citationDay, mapping.trainingDay].filter(Boolean)
  let suspiciousDateValues = 0
  dateFields.forEach((field) => {
    scopedRows.forEach((row) => {
      if (!hasValue(row[field])) {
        return
      }

      const parsedDayKey = parseDayKey(row[field])
      if (!parsedDayKey) {
        return
      }

      const year = Number(parsedDayKey.slice(0, 4))
      if (!Number.isInteger(year) || year < 2000 || year > 2100) {
        suspiciousDateValues += 1
      }
    })
  })
  profile.suspiciousDateValues = suspiciousDateValues

  return profile
}

function compareDataProfiles(previousProfile, currentProfile) {
  if (!previousProfile || !currentProfile) {
    return null
  }

  const previousHeaders = new Set(previousProfile.headers ?? [])
  const currentHeaders = new Set(currentProfile.headers ?? [])

  const addedHeaders = Array.from(currentHeaders).filter((header) => !previousHeaders.has(header))
  const removedHeaders = Array.from(previousHeaders).filter((header) => !currentHeaders.has(header))

  const mappingChanges = Object.keys(currentProfile.mappedColumns ?? {})
    .map((field) => ({
      field,
      previous: String(previousProfile.mappedColumns?.[field] ?? ''),
      current: String(currentProfile.mappedColumns?.[field] ?? ''),
    }))
    .filter((change) => change.previous !== change.current)

  return {
    addedHeaders,
    removedHeaders,
    mappingChanges,
    providerDelta: (currentProfile.totalProviders ?? 0) - (previousProfile.totalProviders ?? 0),
    rowsDelta: (currentProfile.totalRows ?? 0) - (previousProfile.totalRows ?? 0),
  }
}

function buildConsistencyQuestions({
  profile,
  profileDiff,
  snapshotCandidate,
  timelineDecision,
  validationErrors,
  validationWarnings,
}) {
  const questions = []

  if (!profile || !snapshotCandidate) {
    return questions
  }

  if (timelineDecision?.segment === 'legacy') {
    questions.push({
      id: 'legacy-adapt-to-official',
      severity: 'high',
      required: true,
      text: `Este archivo fue clasificado como LEGACY (${Math.round(toSafeNumber(snapshotCandidate.totalProviders))} proveedores). Quieres adaptarlo al universo OFICIAL (${OFFICIAL_TARGET_TOTAL_PROVIDERS}) antes de guardarlo?`,
    })
  }

  if (profileDiff && (profileDiff.addedHeaders.length || profileDiff.removedHeaders.length)) {
    questions.push({
      id: 'schema-shift',
      severity: 'high',
      required: true,
      text: 'Detectamos cambios de columnas vs la ultima carga. Confirmas que el mapeo actual mantiene la logica historica?',
    })
  }

  if (profile.providerCoverageRate > 0 && profile.providerCoverageRate < 0.95) {
    questions.push({
      id: 'provider-coverage',
      severity: 'high',
      required: true,
      text: `Solo ${(profile.providerCoverageRate * 100).toFixed(1)}% de filas tiene ID Proveedor. El archivo esta completo?`,
    })
  }

  if (profile.excludedRowsByScope > 0) {
    questions.push({
      id: 'scope-exclusions',
      severity: 'high',
      required: true,
      text: `Se excluiran ${profile.excludedRowsByScope} filas fuera del alcance del proyecto (grupos 1-9 + sin grupo). Confirmas esta exclusion?`,
    })
  }

  if (profile.duplicateProviders > 0) {
    questions.push({
      id: 'duplicate-providers',
      severity: 'medium',
      required: true,
      text: `Hay ${profile.duplicateProviders} filas duplicadas por ID Proveedor. Deben deduplicarse para este corte?`,
    })
  }

  if (profile.numericStageRatio >= 0.2) {
    questions.push({
      id: 'numeric-stage',
      severity: 'high',
      required: true,
      text: 'Etapas con codigos numericos detectadas (ej: 5009). Confirmas que la API/backend esta decodificada?',
    })
  }

  if (profile.suspiciousDateValues > 0) {
    questions.push({
      id: 'suspicious-dates',
      severity: 'high',
      required: true,
      text: `Se detectaron ${profile.suspiciousDateValues} fechas fuera de rango esperado. Ya validaste esas columnas?`,
    })
  }

  validationErrors.forEach((_, index) => {
    questions.push({
      id: `validation-error-${index}`,
      severity: 'high',
      required: true,
      text: 'Hay errores de validacion del snapshot. Debes corregirlos antes de guardar.',
    })
  })

  if (validationWarnings.length) {
    questions.push({
      id: 'validation-warnings',
      severity: 'medium',
      required: false,
      text: 'Existen alertas de validacion. Confirmas que revisaste el resultado antes de guardar?',
    })
  }

  return questions
}

function normalizeStreakPipelines(items) {
  if (!Array.isArray(items)) {
    return []
  }

  return items
    .map((item) => ({
      key: String(item?.key ?? ''),
      name: String(item?.name ?? item?.key ?? ''),
    }))
    .filter((item) => item.key)
}

async function fetchStreakPipelines() {
  const fallbackUrl = `${STREAK_API_FALLBACK_BASE_URL}/api/streak/pipelines`
  const { response, payload } = await fetchJsonWithFallback(
    STREAK_PIPELINES_API_PATH,
    fallbackUrl,
    (body) => typeof body?.configured === 'boolean' && Array.isArray(body?.pipelines),
  )

  if (!response.ok) {
    throw new Error(resolveApiErrorMessage(payload, 'No se pudieron cargar los pipelines de Streak.'))
  }

  return {
    configured: Boolean(payload.configured),
    message: String(payload.message ?? ''),
    pipelines: normalizeStreakPipelines(payload.pipelines),
  }
}

async function fetchStreakPipelineRows(pipelineKey) {
  const encodedPipelineKey = encodeURIComponent(pipelineKey)
  const primaryUrl = `${STREAK_PIPELINES_API_PATH}/${encodedPipelineKey}/boxes`
  const fallbackUrl = `${STREAK_API_FALLBACK_BASE_URL}/api/streak/pipelines/${encodedPipelineKey}/boxes`
  const { response, payload } = await fetchJsonWithFallback(
    primaryUrl,
    fallbackUrl,
    (body) => Array.isArray(body?.rows) && Array.isArray(body?.headers),
  )

  if (!response.ok) {
    throw new Error(resolveApiErrorMessage(payload, 'No se pudo importar la informacion desde Streak.'))
  }

  const rows = Array.isArray(payload.rows) ? payload.rows : []
  const headers = Array.isArray(payload.headers) ? payload.headers.map((item) => String(item)) : []
  const encodedStageRows = rows
    .slice(0, 120)
    .filter((row) => /^\d{4,}$/.test(String(row?.Etapa ?? '').trim())).length
  const encodedValueRows = rows
    .slice(0, 120)
    .filter((row) =>
      ['Día de Capacitación', 'Enrolado', 'Whatsapp', 'Asistencia'].some((field) =>
        /^\d{4,}$/.test(String(row?.[field] ?? '').trim()),
      ),
    ).length
  const sampleSize = Math.min(rows.length, 120)
  const encodedRatio = sampleSize
    ? (encodedStageRows + encodedValueRows) / (sampleSize * 2)
    : 0
  if (encodedRatio >= 0.45) {
    throw new Error(
      'Importacion bloqueada: el backend de Streak esta devolviendo codigos (5009/9001). Reinicia `npm run dev:api` con la version actual.',
    )
  }
  const pipelineName = String(payload?.pipeline?.name ?? pipelineKey)
  return { rows, headers, pipelineName }
}

function buildGroupContactSummary(rows, mapping, contactStage, trainedStage, officialProviderIdSet) {
  if (!mapping.group || !mapping.providerId) {
    return {
      totalProviders: 0,
      contactedProviders: 0,
      trainedProviders: 0,
      enrolledProviders: 0,
      pendingProviders: 0,
      contactRate: 0,
      trainedRate: 0,
      enrolledRate: 0,
      byGroup: [],
      byGroupTrainedRate: [],
    }
  }

  const byGroupMap = new Map()
  const globalProviderMap = new Map()
  const normalizedContactStage = normalizeText(contactStage)
  const normalizedTrainedStage = normalizeText(trainedStage)

  rows.forEach((row) => {
    if (!hasValue(row[mapping.providerId])) {
      return
    }

    const projectScope = resolveProjectScopeForRow(row, mapping)
    if (!projectScope.included) {
      return
    }

    const providerId = String(row[mapping.providerId]).trim()
    if (officialProviderIdSet?.size && !officialProviderIdSet.has(providerId)) {
      return
    }

    const groupValue = projectScope.groupLabel
    const stageValue = mapping.stage && hasValue(row[mapping.stage]) ? normalizeText(row[mapping.stage]) : ''
    const isEnrolledProvider = mapping.enrolled && isPilotFlag(row[mapping.enrolled])
    const isPilotProvider = mapping.pilot && isPilotFlag(row[mapping.pilot])
    const isFacturandoStage = stageValue.includes('factur')

    if (!byGroupMap.has(groupValue)) {
      byGroupMap.set(groupValue, new Map())
    }

    const providerByGroup = byGroupMap.get(groupValue)
    if (!providerByGroup.has(providerId)) {
      providerByGroup.set(providerId, { contacted: false, trained: false, enrolled: false })
    }

    if (!globalProviderMap.has(providerId)) {
      globalProviderMap.set(providerId, { contacted: false, trained: false, enrolled: false })
    }

    const groupStatus = providerByGroup.get(providerId)
    const globalStatus = globalProviderMap.get(providerId)
    if (normalizedContactStage && stageValue === normalizedContactStage) {
      groupStatus.contacted = true
      globalStatus.contacted = true
    }

    if (normalizedTrainedStage && stageValue === normalizedTrainedStage) {
      groupStatus.trained = true
      groupStatus.contacted = true
      globalStatus.trained = true
      globalStatus.contacted = true
    }

    if (isEnrolledProvider || isPilotProvider || isFacturandoStage) {
      groupStatus.enrolled = true
      globalStatus.enrolled = true
    }
  })

  const byGroup = Array.from(byGroupMap.entries())
    .map(([group, providers]) => {
      const statuses = Array.from(providers.values())
      const total = statuses.length
      const trained = statuses.filter((status) => status.trained).length
      const enrolled = statuses.filter((status) => status.enrolled).length
      const contacted = statuses.filter((status) => status.contacted).length
      const pending = Math.max(total - contacted, 0)
      const contactRate = total ? (contacted / total) * 100 : 0
      const trainedRate = total ? (trained / total) * 100 : 0
      const enrolledRate = total ? (enrolled / total) * 100 : 0

      return {
        group,
        total,
        contacted,
        trained,
        enrolled,
        pending,
        contactRate,
        trainedRate,
        enrolledRate,
      }
    })
    .sort((a, b) => compareGroupLabelsAsc(a.group, b.group))

  const byGroupTrainedRate = byGroup.slice(0, 8).map((row, index) => ({
    label: row.group,
    value: Number(row.trainedRate.toFixed(1)),
    color: COLOR_PALETTE[index % COLOR_PALETTE.length],
  }))

  const globalStatuses = Array.from(globalProviderMap.values())
  const totalProviders = globalStatuses.length
  const trainedProviders = globalStatuses.filter((status) => status.trained).length
  const enrolledProviders = globalStatuses.filter((status) => status.enrolled).length
  const contactedProviders = globalStatuses.filter((status) => status.contacted).length
  const pendingProviders = Math.max(totalProviders - contactedProviders, 0)
  const contactRate = totalProviders ? (contactedProviders / totalProviders) * 100 : 0
  const trainedRate = totalProviders ? (trainedProviders / totalProviders) * 100 : 0
  const enrolledRate = totalProviders ? (enrolledProviders / totalProviders) * 100 : 0

  return {
    totalProviders,
    contactedProviders,
    trainedProviders,
    enrolledProviders,
    pendingProviders,
    contactRate,
    trainedRate,
    enrolledRate,
    byGroup,
    byGroupTrainedRate,
  }
}

function buildRescuedAnalysis(rows, mapping, totalProviders, contactStage, officialProviderIdSet) {
  if (!mapping.providerId || !mapping.rescuedBy) {
    return {
      totalRescued: 0,
      rescueRate: 0,
      scheduledByGiuliano: 0,
      byGroup: [],
      byStage: [],
      byRescuer: [],
    }
  }

  const providersWithGiulianoMark = new Set()

  rows.forEach((row) => {
    const projectScope = resolveProjectScopeForRow(row, mapping)
    if (!projectScope.included) {
      return
    }

    if (!hasValue(row[mapping.providerId]) || !hasValue(row[mapping.rescuedBy])) {
      return
    }

    const providerId = String(row[mapping.providerId]).trim()
    if (officialProviderIdSet?.size && !officialProviderIdSet.has(providerId)) {
      return
    }

    const rescuerValue = String(row[mapping.rescuedBy]).trim()
    if (normalizeText(rescuerValue).includes('giuliano')) {
      providersWithGiulianoMark.add(providerId)
    }
  })

  const providers = new Map()
  const scheduledByGiulianoProviders = new Set()
  const normalizedContactStage = normalizeText(contactStage)

  rows.forEach((row) => {
    const projectScope = resolveProjectScopeForRow(row, mapping)
    if (!projectScope.included) {
      return
    }

    if (!hasValue(row[mapping.providerId])) {
      return
    }

    const providerId = String(row[mapping.providerId]).trim()
    if (officialProviderIdSet?.size && !officialProviderIdSet.has(providerId)) {
      return
    }

    const hasRescuer = hasValue(row[mapping.rescuedBy])
    const hasCitation = mapping.citationDay && hasValue(row[mapping.citationDay])
    const includeScheduledByGiuliano = hasCitation && providersWithGiulianoMark.has(providerId)

    if (!hasRescuer && !includeScheduledByGiuliano) {
      return
    }

    const groupValue = projectScope.groupLabel
    const stageValue =
      mapping.stage && hasValue(row[mapping.stage]) ? String(row[mapping.stage]).trim() : 'Sin etapa'
    const normalizedStageValue =
      mapping.stage && hasValue(row[mapping.stage]) ? normalizeText(row[mapping.stage]) : ''
    const rescuerValue = hasRescuer ? String(row[mapping.rescuedBy]).trim() : 'Giuliano'

    providers.set(providerId, {
      group: groupValue,
      stage: stageValue,
      rescuer: rescuerValue,
    })

    if (
      includeScheduledByGiuliano &&
      normalizedContactStage &&
      normalizedStageValue === normalizedContactStage
    ) {
      scheduledByGiulianoProviders.add(providerId)
    }
  })

  const rescuedUniverse = Array.from(providers.values())
  const totalRescued = rescuedUniverse.length
  const rescueRate = totalProviders ? (totalRescued / totalProviders) * 100 : 0

  const aggregateCounts = (values) => {
    const counts = values.reduce((accumulator, value) => {
      const label = value || 'Sin dato'
      accumulator[label] = (accumulator[label] ?? 0) + 1
      return accumulator
    }, {})

    return Object.entries(counts)
      .map(([label, count]) => ({ label, count }))
      .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
  }

  return {
    totalRescued,
    rescueRate,
    scheduledByGiuliano: scheduledByGiulianoProviders.size,
    byGroup: aggregateCounts(rescuedUniverse.map((item) => item.group))
      .sort((a, b) => compareGroupLabelsAsc(a.label, b.label))
      .slice(0, 8),
    byStage: aggregateCounts(rescuedUniverse.map((item) => item.stage)).slice(0, 8),
    byRescuer: aggregateCounts(rescuedUniverse.map((item) => item.rescuer)).slice(0, 8),
  }
}

function buildCitationAnalysis(rows, mapping, totalProviders, officialProviderIdSet) {
  if (!mapping.providerId) {
    return {
      totalAppointments: 0,
      providersWithCitation: 0,
      trainedByCitation: 0,
      coverageRate: 0,
      appointmentsPerProvider: 0,
      trainingDaysCount: 0,
      trainedByTrainingDay: 0,
      byTrainingDay: [],
      byGroup: [],
      byStage: [],
    }
  }

  const scopedRows = rows.filter((row) => {
    const projectScope = resolveProjectScopeForRow(row, mapping)
    if (!projectScope.included) {
      return false
    }

    if (!officialProviderIdSet?.size || !mapping.providerId || !hasValue(row[mapping.providerId])) {
      return true
    }

    const providerId = String(row[mapping.providerId]).trim()
    return officialProviderIdSet.has(providerId)
  })

  const citationRows = mapping.citationDay
    ? scopedRows.filter((row) => hasValue(row[mapping.providerId]) && hasValue(row[mapping.citationDay]))
    : []

  const totalAppointments = citationRows.length
  const providerSnapshots = new Map()

  citationRows.forEach((row) => {
    const providerId = String(row[mapping.providerId]).trim()
    const projectScope = resolveProjectScopeForRow(row, mapping)
    providerSnapshots.set(providerId, {
      group: projectScope.groupLabel,
      stage:
        mapping.stage && hasValue(row[mapping.stage]) ? String(row[mapping.stage]).trim() : 'Sin etapa',
    })
  })

  const providersWithCitation = providerSnapshots.size
  const appointmentsPerProvider = providersWithCitation
    ? totalAppointments / providersWithCitation
    : 0

  const aggregateCounts = (values) => {
    const counts = values.reduce((accumulator, value) => {
      const label = value || 'Sin dato'
      accumulator[label] = (accumulator[label] ?? 0) + 1
      return accumulator
    }, {})

    return Object.entries(counts)
      .map(([label, count]) => ({ label, count }))
      .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label))
      .slice(0, 8)
  }

  const snapshotValues = Array.from(providerSnapshots.values())

  const trainingDayMap = new Map()
  const trainedProvidersFromDay = new Set()
  const inferredTrainingYear = inferTrainingYear(scopedRows, mapping)

  if (mapping.trainingDay) {
    scopedRows.forEach((row) => {
      if (!hasValue(row[mapping.providerId]) || !hasValue(row[mapping.trainingDay])) {
        return
      }

      const providerId = String(row[mapping.providerId]).trim()
      const dayBuckets = parseTrainingDayBuckets(row[mapping.trainingDay], inferredTrainingYear)
      if (!dayBuckets.length) {
        return
      }

      dayBuckets.forEach(({ bucketKey, label, inferredYear: bucketInferredYear }) => {
        if (!trainingDayMap.has(bucketKey)) {
          trainingDayMap.set(bucketKey, {
            label,
            providers: new Set(),
            inferredYear: Boolean(bucketInferredYear),
          })
        }

        const bucket = trainingDayMap.get(bucketKey)
        bucket.providers.add(providerId)
        bucket.inferredYear = bucket.inferredYear && Boolean(bucketInferredYear)
      })
      trainedProvidersFromDay.add(providerId)
    })
  }

  applyRolloverToInferredDays(trainingDayMap, inferredTrainingYear)

  const byTrainingDay = Array.from(trainingDayMap.entries())
    .sort((a, b) => {
      const sortKeyA = getTrainingBucketSortKey(a[0])
      const sortKeyB = getTrainingBucketSortKey(b[0])
      return sortKeyA.localeCompare(sortKeyB)
    })
    .map(([, bucket]) => ({
      label: bucket.label,
      count: bucket.providers.size,
    }))

  const trainedByTrainingDay = trainedProvidersFromDay.size
  const trainedByCitation = trainedByTrainingDay
  const coverageRate = totalProviders ? (trainedByTrainingDay / totalProviders) * 100 : 0

  return {
    totalAppointments,
    providersWithCitation,
    trainedByCitation,
    coverageRate,
    appointmentsPerProvider,
    trainingDaysCount: trainingDayMap.size,
    trainedByTrainingDay,
    byTrainingDay,
    byGroup: aggregateCounts(snapshotValues.map((item) => item.group)).sort((a, b) =>
      compareGroupLabelsAsc(a.label, b.label),
    ),
    byStage: aggregateCounts(snapshotValues.map((item) => item.stage)),
  }
}

function ExcelDashboardLoader() {
  const xlsxRef = useRef(null)
  const workbookRef = useRef(null)

  const [sheetNames, setSheetNames] = useState([])
  const [selectedSheet, setSelectedSheet] = useState('')
  const [headers, setHeaders] = useState([])
  const [rows, setRows] = useState([])
  const [mapping, setMapping] = useState(INITIAL_MAPPING)
  const [contactStage, setContactStage] = useState('')
  const [trainedStage, setTrainedStage] = useState('')
  const [fileName, setFileName] = useState('')
  const [snapshotDayKey, setSnapshotDayKey] = useState(() => getLocalDayKey())
  const [calendarCursor, setCalendarCursor] = useState(() => parseDayKeyParts(getLocalDayKey()))
  const [error, setError] = useState('')
  const [historyError, setHistoryError] = useState('')
  const [streakError, setStreakError] = useState('')
  const [streakPipelines, setStreakPipelines] = useState([])
  const [streakConfigured, setStreakConfigured] = useState(true)
  const [selectedStreakPipelineKey, setSelectedStreakPipelineKey] = useState('')
  const [isLoadingStreakPipelines, setIsLoadingStreakPipelines] = useState(false)
  const [isImportingFromStreak, setIsImportingFromStreak] = useState(false)
  const [isSavingSnapshot, setIsSavingSnapshot] = useState(false)
  const [isLoadingSnapshotDay, setIsLoadingSnapshotDay] = useState(false)
  const [dataSourceMode, setDataSourceMode] = useState('live')
  const [isLoaderPanelCollapsed, setIsLoaderPanelCollapsed] = useState(false)
  const [saveNotice, setSaveNotice] = useState('')
  const [previousDataProfile, setPreviousDataProfile] = useState(null)
  const [questionAnswers, setQuestionAnswers] = useState({})
  const [dailyHistorySnapshots, setDailyHistorySnapshots] = useState([])
  const [officialProviderUniverseIds, setOfficialProviderUniverseIds] = useState([])
  const currentDataProfileRef = useRef(null)

  const applyParsedRows = (parsedRows, explicitHeaders = []) => {
    if (currentDataProfileRef.current) {
      setPreviousDataProfile(currentDataProfileRef.current)
    }
    setQuestionAnswers({})

    const headerSet = new Set()
    explicitHeaders.forEach((header) => {
      if (header) {
        headerSet.add(header)
      }
    })
    parsedRows.forEach((row) => {
      Object.keys(row ?? {}).forEach((header) => headerSet.add(header))
    })
    const detectedHeaders = Array.from(headerSet)

    setHeaders(detectedHeaders)
    setRows(parsedRows)

    setMapping((previous) => {
      if (!detectedHeaders.length) {
        return INITIAL_MAPPING
      }

      const automatic = inferMapping(detectedHeaders)

      return {
        group: previous.group && detectedHeaders.includes(previous.group) ? previous.group : automatic.group,
        stage: previous.stage && detectedHeaders.includes(previous.stage) ? previous.stage : automatic.stage,
        enrolled:
          previous.enrolled && detectedHeaders.includes(previous.enrolled)
            ? previous.enrolled
            : automatic.enrolled,
        citationDay:
          previous.citationDay && detectedHeaders.includes(previous.citationDay)
            ? previous.citationDay
            : automatic.citationDay,
        trainingDay:
          previous.trainingDay && detectedHeaders.includes(previous.trainingDay)
            ? previous.trainingDay
            : automatic.trainingDay,
        name: previous.name && detectedHeaders.includes(previous.name) ? previous.name : automatic.name,
        providerId:
          previous.providerId && detectedHeaders.includes(previous.providerId)
            ? previous.providerId
            : automatic.providerId,
        rescuedBy:
          previous.rescuedBy && detectedHeaders.includes(previous.rescuedBy)
            ? previous.rescuedBy
            : automatic.rescuedBy,
        pilot:
          previous.pilot && detectedHeaders.includes(previous.pilot)
            ? previous.pilot
            : automatic.pilot,
      }
    })
  }

  const updateSheetData = (xlsxModule, workbook, sheetName) => {
    const worksheet = workbook?.Sheets?.[sheetName]
    if (!worksheet) {
      setHeaders([])
      setRows([])
      return
    }

    const parsedRows = xlsxModule.utils.sheet_to_json(worksheet, { defval: '' })
    applyParsedRows(parsedRows)
  }

  const applyStoredDashboardPayload = useCallback((payload) => {
    if (!payload) {
      return
    }

    if (currentDataProfileRef.current) {
      setPreviousDataProfile(currentDataProfileRef.current)
    }
    setQuestionAnswers({})

    const payloadHeaders = Array.isArray(payload.headers) ? payload.headers : []
    const payloadRows = Array.isArray(payload.rows) ? payload.rows : []
    const payloadMapping =
      payload.mapping && typeof payload.mapping === 'object' && !Array.isArray(payload.mapping)
        ? payload.mapping
        : INITIAL_MAPPING

    const sheetName = String(payload.sheetName ?? '').trim() || 'Snapshot guardado'

    setHeaders(payloadHeaders)
    setRows(payloadRows)
    setMapping({ ...INITIAL_MAPPING, ...payloadMapping })
    setContactStage(String(payload.contactStage ?? ''))
    setTrainedStage(String(payload.trainedStage ?? ''))
    setSheetNames(sheetName ? [sheetName] : [])
    setSelectedSheet(sheetName)
    setFileName(String(payload.fileName ?? '').trim() || 'Snapshot guardado')
    setDataSourceMode('history')
    workbookRef.current = null

    if (isValidDayKey(payload.dayKey)) {
      setSnapshotDayKey(payload.dayKey)
    }
  }, [])

  const handleFileChange = async (event) => {
    const file = event.target.files?.[0]
    if (!file) {
      return
    }

    try {
      setError('')
      setSaveNotice('')
      setFileName(file.name)
      setSnapshotDayKey(inferSnapshotDayKeyFromFile(file))
      const arrayBuffer = await file.arrayBuffer()

      if (!xlsxRef.current) {
        xlsxRef.current = await import('xlsx')
      }

      const workbook = xlsxRef.current.read(arrayBuffer, { type: 'array' })
      workbookRef.current = workbook

      const availableSheets = workbook.SheetNames ?? []
      const firstSheet = availableSheets[0] ?? ''

      setSheetNames(availableSheets)
      setSelectedSheet(firstSheet)
      updateSheetData(xlsxRef.current, workbook, firstSheet)
      setDataSourceMode('live')
    } catch {
      setError('No se pudo leer el archivo Excel. Verifica el formato e intenta de nuevo.')
      setSheetNames([])
      setSelectedSheet('')
      setHeaders([])
      setRows([])
      setMapping(INITIAL_MAPPING)
      setContactStage('')
      setTrainedStage('')
    }
  }

  const handleSheetChange = (event) => {
    const nextSheet = event.target.value
    setSelectedSheet(nextSheet)

    if (xlsxRef.current) {
      if (!workbookRef.current) {
        return
      }
      updateSheetData(xlsxRef.current, workbookRef.current, nextSheet)
    }
  }

  const handleMappingChange = (field) => (event) => {
    const value = event.target.value
    setMapping((previous) => ({ ...previous, [field]: value }))
  }

  const loadStreakPipelines = useCallback(async () => {
    setIsLoadingStreakPipelines(true)
    try {
      const result = await fetchStreakPipelines()
      setStreakConfigured(result.configured)
      setStreakPipelines(result.pipelines)
      setStreakError('')

      if (!result.configured) {
        setStreakError(result.message || 'Debes configurar STREAK_API_KEY en el backend.')
        setSelectedStreakPipelineKey('')
        return
      }

      setSelectedStreakPipelineKey((previous) => {
        if (previous && result.pipelines.some((pipeline) => pipeline.key === previous)) {
          return previous
        }
        return result.pipelines[0]?.key ?? ''
      })
    } catch (loadError) {
      setStreakError(loadError.message)
    } finally {
      setIsLoadingStreakPipelines(false)
    }
  }, [])

  const handleImportFromStreak = async () => {
    if (!selectedStreakPipelineKey || isImportingFromStreak) {
      return
    }

    setIsImportingFromStreak(true)
    try {
      const { rows: importedRows, headers: importedHeaders, pipelineName } =
        await fetchStreakPipelineRows(selectedStreakPipelineKey)
      applyParsedRows(importedRows, importedHeaders)
      setSaveNotice('')
      setFileName(`Streak API - ${pipelineName}`)
      setSnapshotDayKey(getLocalDayKey())
      setSheetNames([])
      setSelectedSheet(pipelineName)
      workbookRef.current = null
      setDataSourceMode('live')
      setError('')
      setStreakError('')
    } catch (importError) {
      setStreakError(importError.message)
    } finally {
      setIsImportingFromStreak(false)
    }
  }

  useEffect(() => {
    let isCancelled = false

    const loadHistory = async () => {
      try {
        const snapshots = await fetchDailyHistorySnapshots()
        if (isCancelled) {
          return
        }

        setDailyHistorySnapshots(snapshots)
        setHistoryError('')
      } catch (loadError) {
        if (isCancelled) {
          return
        }

        setHistoryError(loadError.message)
      }
    }

    void loadHistory()

    return () => {
      isCancelled = true
    }
  }, [])

  useEffect(() => {
    void loadStreakPipelines()
  }, [loadStreakPipelines])

  useEffect(() => {
    if (!isValidDayKey(snapshotDayKey)) {
      return
    }

    const { year, month } = parseDayKeyParts(snapshotDayKey)
    setCalendarCursor((previous) =>
      previous.year === year && previous.month === month ? previous : { year, month },
    )
  }, [snapshotDayKey])

  useEffect(() => {
    let isCancelled = false

    const loadOfficialUniverse = async () => {
      const latestOfficialWithPayload = [...dailyHistorySnapshots]
        .filter((snapshot) => snapshot.hasPayload && snapshot.timelineSegment === 'official')
        .sort((a, b) => b.dayKey.localeCompare(a.dayKey))[0]

      if (!latestOfficialWithPayload) {
        setOfficialProviderUniverseIds([])
        return
      }

      try {
        const payload = await fetchDashboardPayloadByDayKey(latestOfficialWithPayload.dayKey)
        if (isCancelled) {
          return
        }

        const providerUniverse = extractProviderUniverseFromPayload(payload)
        setOfficialProviderUniverseIds(providerUniverse.sort((a, b) => a.localeCompare(b, 'es')))
      } catch {
        if (!isCancelled) {
          setOfficialProviderUniverseIds([])
        }
      }
    }

    void loadOfficialUniverse()

    return () => {
      isCancelled = true
    }
  }, [dailyHistorySnapshots])

  const savedSnapshotDayKeySet = useMemo(
    () => new Set(dailyHistorySnapshots.map((snapshot) => snapshot.dayKey)),
    [dailyHistorySnapshots],
  )
  const savedDashboardDayKeySet = useMemo(
    () =>
      new Set(
        dailyHistorySnapshots
          .filter((snapshot) => snapshot.hasPayload)
          .map((snapshot) => snapshot.dayKey),
      ),
    [dailyHistorySnapshots],
  )

  const calendarMonthLabel = useMemo(() => {
    const date = new Date(Date.UTC(calendarCursor.year, calendarCursor.month - 1, 1))
    return new Intl.DateTimeFormat('es-CL', {
      month: 'long',
      year: 'numeric',
      timeZone: 'UTC',
    }).format(date)
  }, [calendarCursor.month, calendarCursor.year])

  const miniCalendarCells = useMemo(
    () => buildMiniCalendarCells(calendarCursor.year, calendarCursor.month),
    [calendarCursor.month, calendarCursor.year],
  )

  const stageValues = useMemo(() => {
    if (!mapping.stage) {
      return []
    }

    return Array.from(
      new Set(
        rows
          .filter((row) => hasValue(row[mapping.stage]))
          .map((row) => String(row[mapping.stage]).trim()),
      ),
    ).sort((a, b) => a.localeCompare(b))
  }, [rows, mapping.stage])

  useEffect(() => {
    if (!stageValues.length) {
      setContactStage('')
      return
    }

    if (stageValues.includes(contactStage)) {
      return
    }

    setContactStage(resolveDefaultStage(stageValues, ['contactad', 'contact']))
  }, [stageValues, contactStage])

  useEffect(() => {
    if (!stageValues.length) {
      setTrainedStage('')
      return
    }

    if (stageValues.includes(trainedStage)) {
      return
    }

    setTrainedStage(resolveDefaultStage(stageValues, ['capacit', 'entren']))
  }, [stageValues, trainedStage])

  const effectiveOfficialProviderIdSet = useMemo(() => {
    const universe = new Set(officialProviderUniverseIds)
    if (!universe.size) {
      return null
    }

    if (
      mapping.providerId &&
      isValidDayKey(snapshotDayKey) &&
      snapshotDayKey >= OFFICIAL_SEGMENT_START_DAY_KEY
    ) {
      const liveProviderIds = extractProviderIdsFromRows(rows, mapping.providerId, mapping)
      liveProviderIds.forEach((providerId) => universe.add(providerId))
    }

    return universe
  }, [mapping, officialProviderUniverseIds, rows, snapshotDayKey])

  const contactMetrics = useMemo(
    () =>
      buildGroupContactSummary(
        rows,
        mapping,
        contactStage,
        trainedStage,
        effectiveOfficialProviderIdSet,
      ),
    [contactStage, effectiveOfficialProviderIdSet, mapping, rows, trainedStage],
  )

  const rescuedMetrics = useMemo(
    () =>
      buildRescuedAnalysis(
        rows,
        mapping,
        contactMetrics.totalProviders,
        contactStage,
        effectiveOfficialProviderIdSet,
      ),
    [contactMetrics.totalProviders, contactStage, effectiveOfficialProviderIdSet, mapping, rows],
  )
  const citationMetrics = useMemo(
    () =>
      buildCitationAnalysis(
        rows,
        mapping,
        contactMetrics.totalProviders,
        effectiveOfficialProviderIdSet,
      ),
    [contactMetrics.totalProviders, effectiveOfficialProviderIdSet, mapping, rows],
  )

  const snapshotCandidate = useMemo(() => {
    if (
      !rows.length ||
      !mapping.providerId ||
      !contactMetrics.totalProviders ||
      !isValidDayKey(snapshotDayKey)
    ) {
      return null
    }

    return {
      dayKey: snapshotDayKey,
      fileName: fileName || 'Sin archivo',
      sheetName: selectedSheet || 'Sin hoja',
      totalProviders: contactMetrics.totalProviders,
      contactedProviders: contactMetrics.contactedProviders,
      trainedProviders: contactMetrics.trainedProviders,
      enrolledProviders: contactMetrics.enrolledProviders,
      rescuedProviders: rescuedMetrics.totalRescued,
      citedProviders: citationMetrics.providersWithCitation,
      trainingDaysCount: citationMetrics.trainingDaysCount,
      contactRate: Number(contactMetrics.contactRate.toFixed(1)),
      trainedRate: Number(contactMetrics.trainedRate.toFixed(1)),
      rescueRate: Number(rescuedMetrics.rescueRate.toFixed(1)),
    }
  }, [
    citationMetrics.providersWithCitation,
    citationMetrics.trainingDaysCount,
    contactMetrics.contactedProviders,
    contactMetrics.contactRate,
    contactMetrics.enrolledProviders,
    contactMetrics.totalProviders,
    contactMetrics.trainedProviders,
    contactMetrics.trainedRate,
    fileName,
    mapping.providerId,
    rescuedMetrics.rescueRate,
    rescuedMetrics.totalRescued,
    rows.length,
    snapshotDayKey,
    selectedSheet,
  ])

  const snapshotDashboardPayload = useMemo(() => {
    if (!snapshotCandidate) {
      return null
    }

    return {
      dayKey: snapshotCandidate.dayKey,
      fileName: snapshotCandidate.fileName,
      sheetName: snapshotCandidate.sheetName,
      headers,
      rows,
      mapping,
      contactStage,
      trainedStage,
    }
  }, [contactStage, headers, mapping, rows, snapshotCandidate, trainedStage])

  const snapshotInputValidation = useMemo(
    () =>
      validateSnapshotInput(
        rows,
        mapping,
        contactStage,
        trainedStage,
        effectiveOfficialProviderIdSet,
      ),
    [contactStage, effectiveOfficialProviderIdSet, mapping, rows, trainedStage],
  )

  const snapshotTimelineDecision = useMemo(
    () => classifySnapshotTimeline(snapshotCandidate),
    [snapshotCandidate],
  )

  const snapshotChronologyValidation = useMemo(
    () => validateSnapshotChronology(dailyHistorySnapshots, snapshotCandidate),
    [dailyHistorySnapshots, snapshotCandidate],
  )

  const snapshotValidationErrors = useMemo(
    () => [...snapshotInputValidation.errors, ...snapshotChronologyValidation.errors],
    [snapshotChronologyValidation.errors, snapshotInputValidation.errors],
  )

  const snapshotValidationWarnings = useMemo(
    () => [...snapshotInputValidation.warnings, ...snapshotChronologyValidation.warnings],
    [snapshotChronologyValidation.warnings, snapshotInputValidation.warnings],
  )

  const dataQualityProfile = useMemo(
    () =>
      buildDataQualityProfile({
        rows,
        headers,
        mapping,
        stageValues,
        snapshotDayKey,
        officialProviderIdSet: effectiveOfficialProviderIdSet,
      }),
    [effectiveOfficialProviderIdSet, headers, mapping, rows, snapshotDayKey, stageValues],
  )

  const profileDiff = useMemo(
    () => compareDataProfiles(previousDataProfile, dataQualityProfile),
    [dataQualityProfile, previousDataProfile],
  )

  const consistencyQuestions = useMemo(
    () =>
      buildConsistencyQuestions({
        profile: dataQualityProfile,
        profileDiff,
        snapshotCandidate,
        timelineDecision: snapshotTimelineDecision,
        validationErrors: snapshotValidationErrors,
        validationWarnings: snapshotValidationWarnings,
      }),
    [
      dataQualityProfile,
      profileDiff,
      snapshotCandidate,
      snapshotTimelineDecision,
      snapshotValidationErrors,
      snapshotValidationWarnings,
    ],
  )

  const timelineExplanation = useMemo(
    () => buildTimelineExplanation(snapshotTimelineDecision, snapshotCandidate),
    [snapshotCandidate, snapshotTimelineDecision],
  )

  useEffect(() => {
    currentDataProfileRef.current = dataQualityProfile
  }, [dataQualityProfile])

  const requiredQuestions = useMemo(
    () => consistencyQuestions.filter((question) => question.required),
    [consistencyQuestions],
  )

  const unansweredRequiredQuestions = useMemo(
    () =>
      requiredQuestions.filter(
        (question) => !questionAnswers[question.id] || questionAnswers[question.id] === 'pending',
      ),
    [questionAnswers, requiredQuestions],
  )

  const canSaveSnapshot = Boolean(snapshotCandidate) && !isSavingSnapshot

  const saveBlockingReasons = useMemo(() => {
    const reasons = []

    if (!snapshotCandidate) {
      reasons.push('No hay datos listos para guardar (revisa archivo, mapeo y fecha).')
      return reasons
    }

    if (snapshotValidationErrors.length) {
      reasons.push(`Hay ${snapshotValidationErrors.length} error(es) de validación por corregir.`)
    }

    if (unansweredRequiredQuestions.length) {
      reasons.push(
        `Faltan ${unansweredRequiredQuestions.length} pregunta(s) requerida(s) en auditoría.`,
      )
    }

    return reasons
  }, [snapshotCandidate, snapshotValidationErrors.length, unansweredRequiredQuestions.length])

  const handleSaveSnapshot = async () => {
    if (!snapshotCandidate) {
      return
    }

    if (snapshotValidationErrors.length) {
      setHistoryError('No se puede guardar: revisa los errores de validacion del snapshot.')
      return
    }

    if (unansweredRequiredQuestions.length) {
      setHistoryError('Responde las preguntas de consistencia requeridas antes de guardar.')
      return
    }

    try {
      setIsSavingSnapshot(true)
      const snapshots = await saveDailyHistorySnapshot(snapshotCandidate, snapshotDashboardPayload)
      setDailyHistorySnapshots(snapshots)
      setHistoryError('')
      setSaveNotice(
        `Snapshot guardado para ${formatDayLabel(snapshotCandidate.dayKey)} (${snapshotTimelineDecision.segment.toUpperCase()}).`,
      )
    } catch (persistError) {
      setHistoryError(persistError.message)
    } finally {
      setIsSavingSnapshot(false)
    }
  }

  const handleClearDailyHistory = async () => {
    if (!dailyHistorySnapshots.length) {
      return
    }

    const shouldClear = window.confirm('Se borrara el historial evolutivo guardado. Quieres continuar?')
    if (!shouldClear) {
      return
    }

    try {
      await clearDailyHistorySnapshots()
      setDailyHistorySnapshots([])
      setHistoryError('')
    } catch (clearError) {
      setHistoryError(clearError.message)
    }
  }

  const handleSnapshotDateChange = (event) => {
    const value = String(event.target.value ?? '')
    setSnapshotDayKey(value)
  }

  const handleShiftCalendarMonth = (monthOffset) => () => {
    setCalendarCursor((previous) => shiftCalendarMonthCursor(previous, monthOffset))
  }

  const handleSelectCalendarDay = (dayKey) => () => {
    setSnapshotDayKey(dayKey)
  }

  const handleLoadDashboardFromSnapshotDate = async () => {
    if (!isValidDayKey(snapshotDayKey)) {
      setHistoryError('Debes seleccionar una fecha snapshot valida (YYYY-MM-DD).')
      return
    }

    if (!savedSnapshotDayKeySet.has(snapshotDayKey)) {
      setHistoryError(
        `No hay snapshot guardado para ${formatDayLabel(snapshotDayKey)}. Selecciona un dia marcado en el calendario.`,
      )
      return
    }

    if (!savedDashboardDayKeySet.has(snapshotDayKey)) {
      setHistoryError(
        `No hay dashboard completo guardado para ${formatDayLabel(snapshotDayKey)}. Debes guardar ese dia nuevamente con la version actual.`,
      )
      return
    }

    try {
      setIsLoadingSnapshotDay(true)
      const payload = await fetchDashboardPayloadByDayKey(snapshotDayKey)
      applyStoredDashboardPayload(payload)
      setHistoryError('')
      setSaveNotice(`Dashboard cargado desde ${formatDayLabel(snapshotDayKey)} usando datos guardados en base de datos.`)
    } catch (loadError) {
      setHistoryError(loadError.message)
    } finally {
      setIsLoadingSnapshotDay(false)
    }
  }

  const handleQuestionAnswerChange = (questionId) => (event) => {
    const value = String(event.target.value ?? 'pending')
    setQuestionAnswers((previous) => ({ ...previous, [questionId]: value }))
  }

  const handleExportDailyHistoryCsv = () => {
    if (!dailyHistorySnapshots.length) {
      return
    }

    const escapeCsvField = (value) => `"${String(value ?? '').replace(/"/g, '""')}"`
    const rowsForCsv = [
      [
        'Fecha',
        'Archivo',
        'Hoja',
        'Proveedores totales',
        'Contactados',
        'Capacitados',
        'Enrolados',
        'Rescatados',
        'Citados',
        'Dias con capacitacion',
        '% Contactados',
        '% Capacitados',
        '% Rescate',
        'Segmento timeline',
        'Motivo timeline',
      ],
      ...dailyHistorySnapshots.map((snapshot) => [
        formatDayLabel(snapshot.dayKey),
        snapshot.fileName,
        snapshot.sheetName,
        snapshot.totalProviders,
        snapshot.contactedProviders,
        snapshot.trainedProviders,
        snapshot.enrolledProviders,
        snapshot.rescuedProviders,
        snapshot.citedProviders,
        snapshot.trainingDaysCount,
        `${snapshot.contactRate.toFixed(1)}%`,
        `${snapshot.trainedRate.toFixed(1)}%`,
        `${snapshot.rescueRate.toFixed(1)}%`,
        snapshot.timelineSegment ?? '',
        snapshot.timelineReason ?? '',
      ]),
    ]

    const csvContent = rowsForCsv.map((line) => line.map(escapeCsvField).join(';')).join('\n')
    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = 'historial-evolutivo-diario.csv'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  const dashboardKpis = [
    {
      title: 'Proveedores totales',
      value: String(contactMetrics.totalProviders),
      trend: '',
      description: formatDashboardSourceLabel(fileName, snapshotDayKey),
    },
    {
      title: 'Proveedores contactados',
      value: String(contactMetrics.contactedProviders),
      trend: `${contactMetrics.contactRate.toFixed(1)}%`,
      description: 'incluye capacitados',
    },
    {
      title: 'Proveedores capacitados',
      value: String(contactMetrics.trainedProviders),
      trend: `${contactMetrics.trainedRate.toFixed(1)}%`,
      description: `faltan por contactar: ${contactMetrics.pendingProviders}`,
    },
    {
      title: 'Proveedores rescatados',
      value: String(rescuedMetrics.totalRescued),
      trend: `${rescuedMetrics.rescueRate.toFixed(1)}%`,
      description: 'universo con campo Rescatado por',
    },
  ]

  const todayDayKey = getLocalDayKey()
  const selectedDayHasSnapshot = savedSnapshotDayKeySet.has(snapshotDayKey)
  const selectedDayHasDashboard = savedDashboardDayKeySet.has(snapshotDayKey)
  const canLoadDashboardFromDay =
    isValidDayKey(snapshotDayKey) && selectedDayHasSnapshot

  return (
    <div
      className={`excel-loader-layout ${
        isLoaderPanelCollapsed ? 'excel-loader-layout--panel-collapsed' : ''
      }`}
    >
      <section
        className={`excel-loader-panel ${
          isLoaderPanelCollapsed ? 'excel-loader-panel--collapsed' : ''
        }`}
      >
        <div className="excel-loader-panel__head">
          <div>
            <h2>{isLoaderPanelCollapsed ? 'Carga' : 'Cargar Excel'}</h2>
            {!isLoaderPanelCollapsed ? (
              <p>Dashboard por Grupo usando Etapa e ID Proveedor.</p>
            ) : null}
          </div>
          <button
            type="button"
            className="excel-loader-panel__toggle"
            onClick={() => setIsLoaderPanelCollapsed((previous) => !previous)}
            aria-label={
              isLoaderPanelCollapsed ? 'Expandir panel de carga' : 'Colapsar panel de carga'
            }
            title={isLoaderPanelCollapsed ? 'Expandir panel' : 'Colapsar hacia la izquierda'}
          >
            {isLoaderPanelCollapsed ? '>' : '<'}
          </button>
        </div>

        {!isLoaderPanelCollapsed ? (
          <>

        <div className="excel-loader-source">
          <h3>Streak API</h3>
          <label className="excel-loader-field">
            <span>Pipeline</span>
            <select
              value={selectedStreakPipelineKey}
              onChange={(event) => setSelectedStreakPipelineKey(event.target.value)}
              disabled={!streakConfigured || isLoadingStreakPipelines || !streakPipelines.length}
            >
              {!streakPipelines.length ? (
                <option value="">
                  {isLoadingStreakPipelines ? 'Cargando...' : 'Sin pipelines disponibles'}
                </option>
              ) : null}
              {streakPipelines.map((pipeline) => (
                <option key={pipeline.key} value={pipeline.key}>
                  {pipeline.name}
                </option>
              ))}
            </select>
          </label>

          <div className="excel-loader-actions">
            <button
              type="button"
              className="excel-loader-btn"
              onClick={handleImportFromStreak}
              disabled={!selectedStreakPipelineKey || !streakConfigured || isImportingFromStreak}
            >
              {isImportingFromStreak ? 'Importando...' : 'Importar desde Streak'}
            </button>
            <button
              type="button"
              className="excel-loader-btn excel-loader-btn--secondary"
              onClick={loadStreakPipelines}
              disabled={isLoadingStreakPipelines}
            >
              {isLoadingStreakPipelines ? 'Actualizando...' : 'Actualizar'}
            </button>
          </div>
          {streakError ? <p className="excel-loader-error">{streakError}</p> : null}
        </div>

        <label className="excel-loader-field">
          <span>Archivo (.xlsx / .xls)</span>
          <input type="file" accept=".xlsx,.xls" onChange={handleFileChange} />
        </label>

        <label className="excel-loader-field">
          <span>Fecha snapshot</span>
          <input type="date" value={snapshotDayKey} onChange={handleSnapshotDateChange} />
        </label>
        <div className="excel-loader-mini-calendar">
          <div className="excel-loader-mini-calendar__header">
            <button
              type="button"
              className="excel-loader-mini-calendar__nav"
              onClick={handleShiftCalendarMonth(-1)}
              aria-label="Mes anterior"
            >
              ‹
            </button>
            <strong>{calendarMonthLabel}</strong>
            <button
              type="button"
              className="excel-loader-mini-calendar__nav"
              onClick={handleShiftCalendarMonth(1)}
              aria-label="Mes siguiente"
            >
              ›
            </button>
          </div>
          <div className="excel-loader-mini-calendar__weekdays">
            {['L', 'M', 'M', 'J', 'V', 'S', 'D'].map((weekday, index) => (
              <span key={`${weekday}-${index}`}>{weekday}</span>
            ))}
          </div>
          <div className="excel-loader-mini-calendar__grid">
            {miniCalendarCells.map((cell) => {
              const isSelected = cell.dayKey === snapshotDayKey
              const isToday = cell.dayKey === todayDayKey
              const hasSnapshot = savedSnapshotDayKeySet.has(cell.dayKey)
              const hasDashboard = savedDashboardDayKeySet.has(cell.dayKey)

              return (
                <button
                  key={cell.dayKey}
                  type="button"
                  className={[
                    'excel-loader-mini-calendar__day',
                    !cell.inCurrentMonth ? 'excel-loader-mini-calendar__day--outside' : '',
                    isSelected ? 'excel-loader-mini-calendar__day--selected' : '',
                    hasSnapshot ? 'excel-loader-mini-calendar__day--saved' : '',
                    hasDashboard ? 'excel-loader-mini-calendar__day--payload' : '',
                    isToday ? 'excel-loader-mini-calendar__day--today' : '',
                  ]
                    .filter(Boolean)
                    .join(' ')}
                  onClick={handleSelectCalendarDay(cell.dayKey)}
                  title={`${formatDayLabel(cell.dayKey)}${
                    hasDashboard
                      ? ' (dashboard completo guardado)'
                      : hasSnapshot
                        ? ' (solo snapshot: vuelve a guardar para habilitar carga completa)'
                        : ''
                  }`}
                >
                  {cell.dayNumber}
                </button>
              )
            })}
          </div>
          <div className="excel-loader-mini-calendar__legend">
            <span>
              <i className="excel-loader-mini-calendar__dot excel-loader-mini-calendar__dot--snapshot" />
              snapshot
            </span>
            <span>
              <i className="excel-loader-mini-calendar__dot excel-loader-mini-calendar__dot--payload" />
              dashboard completo
            </span>
          </div>
        </div>
        <p className="excel-loader-hint">
          Se detecta desde la fecha del archivo cargado y puedes ajustarla manualmente.
        </p>
        {dataSourceMode === 'history' ? (
          <p className="excel-loader-hint">Vista actual: datos historicos guardados en base de datos.</p>
        ) : null}

        <button
          type="button"
          className="excel-loader-btn excel-loader-btn--secondary"
          onClick={handleLoadDashboardFromSnapshotDate}
          disabled={!canLoadDashboardFromDay || isLoadingSnapshotDay}
        >
          {isLoadingSnapshotDay
            ? 'Cargando dashboard de la fecha...'
            : 'Cargar dashboard de esta fecha'}
        </button>
        {!canLoadDashboardFromDay ? (
          <p className="excel-loader-hint">
            Para cargar por fecha, elige un dia con snapshot guardado (marca celeste).
          </p>
        ) : !selectedDayHasDashboard ? (
          <p className="excel-loader-hint">
            Este dia tiene snapshot pero no dashboard completo. Debes reabrir ese dia y guardar nuevamente.
          </p>
        ) : null}
        {isValidDayKey(snapshotDayKey) ? (
          <p className="excel-loader-hint">
            Estado fecha {formatDayLabel(snapshotDayKey)}: {selectedDayHasSnapshot ? 'snapshot OK' : 'sin snapshot'}
            {' | '}
            {selectedDayHasDashboard ? 'dashboard completo OK' : 'sin dashboard completo'}
          </p>
        ) : null}

        <button
          type="button"
          className="excel-loader-btn"
          onClick={handleSaveSnapshot}
          disabled={!canSaveSnapshot}
        >
          {isSavingSnapshot ? 'Guardando snapshot...' : 'Guardar snapshot validado'}
        </button>
        {saveBlockingReasons.length ? (
          <p className="excel-loader-hint">{saveBlockingReasons.join(' ')}</p>
        ) : null}

        {saveNotice ? <p className="excel-loader-success">{saveNotice}</p> : null}
        {historyError ? <p className="excel-loader-error">{historyError}</p> : null}

        <div className="excel-loader-audit">
          <h3>Auditoria de consistencia</h3>
          <p>
            Filas: {dataQualityProfile.totalRows} | En alcance proyecto: {dataQualityProfile.scopedRows} |
            Excluidas: {dataQualityProfile.excludedRowsByScope}
          </p>
          <p>
            Proveedores unicos: {dataQualityProfile.totalProviders} | Cobertura ID:{' '}
            {(dataQualityProfile.providerCoverageRate * 100).toFixed(1)}%
          </p>
          {dataQualityProfile.officialUniverseSize ? (
            <p>
              Universo oficial IDs: {dataQualityProfile.officialUniverseSize} | Presentes en archivo:{' '}
              {dataQualityProfile.officialUniverseMatched} | Faltantes:{' '}
              {dataQualityProfile.officialUniverseMissing}
            </p>
          ) : null}
          <p>
            Duplicados ID: {dataQualityProfile.duplicateProviders} | Etapas numericas:{' '}
            {(dataQualityProfile.numericStageRatio * 100).toFixed(1)}% | Fechas sospechosas:{' '}
            {dataQualityProfile.suspiciousDateValues}
          </p>
          {snapshotCandidate ? (
            <p>
              Clasificacion automatica: <strong>{snapshotTimelineDecision.segment.toUpperCase()}</strong> |{' '}
              {snapshotTimelineDecision.reason}
            </p>
          ) : null}
          {snapshotCandidate && timelineExplanation ? <p>{timelineExplanation}</p> : null}

          {profileDiff ? (
            <p>
              Cambios vs carga previa (sesion actual): +{profileDiff.addedHeaders.length} columnas / -
              {profileDiff.removedHeaders.length} columnas / {profileDiff.mappingChanges.length} cambios de mapeo
            </p>
          ) : null}

          {consistencyQuestions.length ? (
            <div className="excel-loader-questions">
              <strong>Preguntas de control ({unansweredRequiredQuestions.length} requeridas pendientes)</strong>
              {consistencyQuestions.map((question) => (
                <label key={question.id} className="excel-loader-question">
                  <span>
                    [{question.severity.toUpperCase()}] {question.text}
                  </span>
                  <select
                    value={questionAnswers[question.id] ?? 'pending'}
                    onChange={handleQuestionAnswerChange(question.id)}
                  >
                    <option value="pending">Pendiente</option>
                    <option value="yes">Si</option>
                    <option value="no">No</option>
                    <option value="na">No aplica</option>
                  </select>
                </label>
              ))}
            </div>
          ) : null}
        </div>

        {snapshotValidationErrors.length ? (
          <div className="excel-loader-validation excel-loader-validation--error">
            <strong>Errores de validacion</strong>
            <ul>
              {snapshotValidationErrors.map((message, index) => (
                <li key={`error-${index}`}>{message}</li>
              ))}
            </ul>
          </div>
        ) : null}

        {snapshotValidationWarnings.length ? (
          <div className="excel-loader-validation excel-loader-validation--warn">
            <strong>Alertas de validacion</strong>
            <ul>
              {snapshotValidationWarnings.map((message, index) => (
                <li key={`warn-${index}`}>{message}</li>
              ))}
            </ul>
          </div>
        ) : null}

        {sheetNames.length ? (
          <label className="excel-loader-field">
            <span>Hoja</span>
            <select value={selectedSheet} onChange={handleSheetChange}>
              {sheetNames.map((sheet) => (
                <option key={sheet} value={sheet}>
                  {sheet}
                </option>
              ))}
            </select>
          </label>
        ) : null}

        {headers.length ? (
          <div className="excel-loader-mapping">
            <h3>Mapeo de columnas</h3>
            {Object.keys(FIELD_LABELS).map((field) => (
              <label key={field} className="excel-loader-field">
                <span>{FIELD_LABELS[field]}</span>
                <select value={mapping[field]} onChange={handleMappingChange(field)}>
                  <option value="">Sin asignar</option>
                  {headers.map((header) => (
                    <option key={`${field}-${header}`} value={header}>
                      {getHeaderDisplayName(header)}
                    </option>
                  ))}
                </select>
              </label>
            ))}

            {stageValues.length ? (
              <>
                <label className="excel-loader-field">
                  <span>Etapa que cuenta como contactado</span>
                  <select value={contactStage} onChange={(event) => setContactStage(event.target.value)}>
                    {stageValues.map((stageOption) => (
                      <option key={`contact-${stageOption}`} value={stageOption}>
                        {stageOption}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="excel-loader-field">
                  <span>Etapa que cuenta como capacitado</span>
                  <select value={trainedStage} onChange={(event) => setTrainedStage(event.target.value)}>
                    {stageValues.map((stageOption) => (
                      <option key={`trained-${stageOption}`} value={stageOption}>
                        {stageOption}
                      </option>
                    ))}
                  </select>
                </label>
              </>
            ) : null}
          </div>
        ) : null}

        {error ? <p className="excel-loader-error">{error}</p> : null}
          </>
        ) : null}
      </section>

      <ExecutiveDashboard
        title="Dashboard de contacto y capacitacion por grupo"
        subtitle="Cantidad y porcentaje de proveedores contactados y capacitados"
        kpis={dashboardKpis}
        progressCard={{
          title: 'Cobertura global',
          items: [
            {
              title: 'Contactados',
              currentValue: contactMetrics.contactedProviders,
              targetValue: contactMetrics.totalProviders,
            },
            {
              title: 'Capacitados',
              currentValue: contactMetrics.trainedProviders,
              targetValue: contactMetrics.totalProviders,
            },
            {
              title: 'Enrolados',
              currentValue: contactMetrics.enrolledProviders,
              targetValue: contactMetrics.totalProviders,
            },
          ],
        }}
        barChartTitle="% capacitados por grupo"
        barChartData={contactMetrics.byGroupTrainedRate}
        barChartMaxValue={100}
        barChartFormatValue={(value) => `${value.toFixed(1)}%`}
        extraContent={
          <>
            <EvolutionHistoryCard
              snapshots={dailyHistorySnapshots}
              selectedDayKey={snapshotDayKey}
              onClearHistory={handleClearDailyHistory}
              onExportHistoryCsv={handleExportDailyHistoryCsv}
            />
            <GroupContactTable
              rows={contactMetrics.byGroup}
              contactStage={contactStage}
              trainedStage={trainedStage}
              summary={{
                totalProviders: contactMetrics.totalProviders,
                contactedProviders: contactMetrics.contactedProviders,
                trainedProviders: contactMetrics.trainedProviders,
                pendingProviders: contactMetrics.pendingProviders,
                contactRate: contactMetrics.contactRate,
                trainedRate: contactMetrics.trainedRate,
              }}
            />
            <RescuedAnalysisCard data={rescuedMetrics} />
            <CitationAnalysisCard data={citationMetrics} />
          </>
        }
      />
    </div>
  )
}

export default ExcelDashboardLoader
