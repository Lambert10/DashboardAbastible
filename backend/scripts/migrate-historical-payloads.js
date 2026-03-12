/* global process */

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import Database from 'better-sqlite3'
import xlsx from 'xlsx'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const projectRoot = path.resolve(__dirname, '..', '..')
const defaultDatabasePath = path.join(projectRoot, 'backend', 'data', 'dashboard-history.sqlite')
const defaultInputDirectory = path.join(os.homedir(), 'Downloads')

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

function pad2(value) {
  return String(value).padStart(2, '0')
}

function buildDayKey(year, month, day) {
  return `${year}-${pad2(month)}-${pad2(day)}`
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

function toLocalDayKey(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return ''
  }

  return buildDayKey(date.getFullYear(), date.getMonth() + 1, date.getDate())
}

function inferDayKeyFromNumericDateParts(firstRaw, secondRaw, yearRaw) {
  const first = Number(firstRaw)
  const second = Number(secondRaw)
  let year = Number(yearRaw)

  if (!Number.isInteger(first) || !Number.isInteger(second) || !Number.isInteger(year)) {
    return ''
  }

  if (year < 100) {
    year += 2000
  }

  const monthDayYear = buildDayKey(year, first, second)
  const dayMonthYear = buildDayKey(year, second, first)
  const monthDayValid = isValidDayKey(monthDayYear)
  const dayMonthValid = isValidDayKey(dayMonthYear)

  if (first > 12 && second <= 12) {
    return dayMonthValid ? dayMonthYear : ''
  }

  if (second > 12 && first <= 12) {
    return monthDayValid ? monthDayYear : ''
  }

  if (monthDayValid) {
    return monthDayYear
  }

  if (dayMonthValid) {
    return dayMonthYear
  }

  return ''
}

function inferDayKeyFromFileName(fileName) {
  const baseName = String(fileName ?? '').replace(/\.[^.]+$/, '')
  if (!baseName) {
    return ''
  }

  const yearFirstMatch = baseName.match(/\b(19\d{2}|20\d{2})[-._](\d{1,2})[-._](\d{1,2})\b/)
  if (yearFirstMatch) {
    const parsed = buildDayKey(
      Number(yearFirstMatch[1]),
      Number(yearFirstMatch[2]),
      Number(yearFirstMatch[3]),
    )
    if (isValidDayKey(parsed)) {
      return parsed
    }
  }

  const commonNumericMatch = baseName.match(/\b(\d{1,2})[-._](\d{1,2})[-._](\d{2,4})\b/)
  if (commonNumericMatch) {
    return inferDayKeyFromNumericDateParts(
      commonNumericMatch[1],
      commonNumericMatch[2],
      commonNumericMatch[3],
    )
  }

  return ''
}

function parseArgs(argv) {
  const options = {
    dbPath: defaultDatabasePath,
    inputDirectory: defaultInputDirectory,
    dryRun: false,
    dayFilter: '',
  }

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (arg === '--dry-run') {
      options.dryRun = true
      continue
    }

    if ((arg === '--db' || arg === '-d') && argv[index + 1]) {
      options.dbPath = path.resolve(argv[index + 1])
      index += 1
      continue
    }

    if ((arg === '--dir' || arg === '-i') && argv[index + 1]) {
      options.inputDirectory = path.resolve(argv[index + 1])
      index += 1
      continue
    }

    if ((arg === '--day' || arg === '-k') && argv[index + 1]) {
      options.dayFilter = String(argv[index + 1]).trim()
      index += 1
    }
  }

  return options
}

function scanCandidateExcelFiles(inputDirectory) {
  if (!fs.existsSync(inputDirectory)) {
    return []
  }

  return fs
    .readdirSync(inputDirectory, { withFileTypes: true })
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => /\.(xlsx|xls)$/i.test(name))
    .filter((name) => /streak export/i.test(name) && /portal abastible/i.test(name))
    .map((name) => path.join(inputDirectory, name))
}

function buildFileIndexByDayKey(filePaths) {
  const byDayKey = new Map()

  filePaths.forEach((filePath) => {
    const fileName = path.basename(filePath)
    const stats = fs.statSync(filePath)
    const inferredFromName = inferDayKeyFromFileName(fileName)
    const inferredFromMetadata = toLocalDayKey(stats.mtime)
    const dayKey = isValidDayKey(inferredFromName) ? inferredFromName : inferredFromMetadata
    if (!isValidDayKey(dayKey)) {
      return
    }

    const current = byDayKey.get(dayKey)
    if (!current || stats.mtimeMs > current.mtimeMs) {
      byDayKey.set(dayKey, {
        filePath,
        fileName,
        dayKey,
        mtimeMs: stats.mtimeMs,
      })
    }
  })

  return byDayKey
}

function loadPayloadFromExcel(filePath) {
  const workbook = xlsx.readFile(filePath, { raw: false })
  const selectedSheet =
    workbook.SheetNames.find((sheetName) => normalizeText(sheetName).includes('boxes')) ??
    workbook.SheetNames[0] ??
    ''
  const worksheet = workbook.Sheets[selectedSheet]
  if (!worksheet) {
    return null
  }

  const rows = xlsx.utils.sheet_to_json(worksheet, { defval: '' })
  if (!rows.length) {
    return null
  }

  const headerSet = new Set()
  rows.forEach((row) => {
    Object.keys(row ?? {}).forEach((header) => headerSet.add(String(header)))
  })
  const headers = Array.from(headerSet)
  const mapping = inferMapping(headers)
  const stageValues = mapping.stage
    ? Array.from(
        new Set(
          rows
            .filter((row) => hasValue(row[mapping.stage]))
            .map((row) => String(row[mapping.stage]).trim()),
        ),
      ).sort((first, second) => first.localeCompare(second, 'es'))
    : []

  return {
    headers,
    rows,
    mapping,
    contactStage: resolveDefaultStage(stageValues, ['contactad', 'contact']),
    trainedStage: resolveDefaultStage(stageValues, ['capacit', 'entren']),
    sheetName: selectedSheet,
    fileName: path.basename(filePath),
  }
}

function ensurePayloadTable(database) {
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
}

function runMigration() {
  const options = parseArgs(process.argv.slice(2))
  if (options.dayFilter && !isValidDayKey(options.dayFilter)) {
    console.error(`[migrate-payloads] --day invalido: ${options.dayFilter}`)
    process.exit(1)
  }

  console.log(`[migrate-payloads] DB: ${options.dbPath}`)
  console.log(`[migrate-payloads] Input dir: ${options.inputDirectory}`)
  console.log(`[migrate-payloads] Dry run: ${options.dryRun ? 'yes' : 'no'}`)

  const database = new Database(options.dbPath)
  ensurePayloadTable(database)

  const snapshotRows = database
    .prepare('SELECT day_key AS dayKey FROM daily_history_snapshots ORDER BY day_key')
    .all()
    .map((row) => String(row.dayKey))
    .filter((dayKey) => (!options.dayFilter ? true : dayKey === options.dayFilter))

  if (!snapshotRows.length) {
    console.log('[migrate-payloads] No hay snapshots para migrar.')
    database.close()
    return
  }

  const filePaths = scanCandidateExcelFiles(options.inputDirectory)
  const filesByDayKey = buildFileIndexByDayKey(filePaths)
  console.log(`[migrate-payloads] Excel detectados: ${filePaths.length}`)

  const upsertPayload = database.prepare(`
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

  let migratedCount = 0
  let skippedCount = 0

  snapshotRows.forEach((dayKey) => {
    const file = filesByDayKey.get(dayKey)
    if (!file) {
      skippedCount += 1
      console.log(`[SKIP] ${dayKey} -> sin archivo coincidente en ${options.inputDirectory}`)
      return
    }

    const payload = loadPayloadFromExcel(file.filePath)
    if (!payload) {
      skippedCount += 1
      console.log(`[SKIP] ${dayKey} -> archivo sin filas utiles (${file.fileName})`)
      return
    }

    const payloadRow = {
      dayKey,
      fileName: payload.fileName,
      sheetName: payload.sheetName,
      headersJson: JSON.stringify(payload.headers),
      rowsJson: JSON.stringify(payload.rows),
      mappingJson: JSON.stringify(payload.mapping),
      contactStage: payload.contactStage,
      trainedStage: payload.trainedStage,
    }

    if (!options.dryRun) {
      upsertPayload.run(payloadRow)
    }

    migratedCount += 1
    console.log(
      `[OK] ${dayKey} -> ${payload.fileName} | filas: ${payload.rows.length} | columnas: ${payload.headers.length}`,
    )
  })

  console.log(
    `[migrate-payloads] Resultado: migrados=${migratedCount}, omitidos=${skippedCount}, snapshots=${snapshotRows.length}`,
  )
  database.close()
}

runMigration()
