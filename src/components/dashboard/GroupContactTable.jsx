import './GroupContactTable.css'

function formatSignedDelta(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return '+0'
  }

  if (numeric === 0) {
    return '+0'
  }

  const sign = numeric > 0 ? '+' : ''
  return `${sign}${numeric}`
}

function formatDayLabel(dayKey) {
  const [year, month, day] = String(dayKey ?? '').split('-')
  if (!year || !month || !day) {
    return ''
  }

  return `${day}/${month}/${year}`
}

function normalizeGroupKey(value) {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim()
}

function renderMetricWithDelta(value, delta, hasComparison) {
  const numericValue = Number(value)
  const numericDelta = Number(delta)
  const hasPositiveDelta = hasComparison && Number.isFinite(numericDelta) && numericDelta > 0
  const valueDisplay = Number.isFinite(numericValue) ? numericValue : 0

  return (
    <span className="group-contact-table__metric">
      <span className="group-contact-table__metric-value">{valueDisplay}</span>
      <span
        className={`group-contact-table__metric-change ${hasPositiveDelta ? 'group-contact-table__metric-change--up' : 'group-contact-table__metric-change--empty'}`}
      >
        {hasPositiveDelta ? (
          <>
            <span>{formatSignedDelta(numericDelta)}</span>
            <i className="group-contact-table__delta-arrow" aria-hidden="true" />
          </>
        ) : (
          <span className="group-contact-table__metric-change-placeholder">+0</span>
        )}
      </span>
    </span>
  )
}

function GroupContactTable({ rows, contactStage, trainedStage, summary, comparison }) {
  const totalProviders = summary?.totalProviders ?? 0
  const contactedProviders = summary?.contactedProviders ?? 0
  const trainedProviders = summary?.trainedProviders ?? 0
  const pendingProviders = summary?.pendingProviders ?? 0
  const contactRate = summary?.contactRate ?? 0
  const trainedRate = summary?.trainedRate ?? 0
  const hasComparison = Boolean(comparison?.dayKey)
  const comparisonDayLabel = formatDayLabel(comparison?.dayKey)
  const comparisonRowsByGroup = new Map(
    (comparison?.rows ?? []).map((row) => [normalizeGroupKey(row.group), row]),
  )
  const contactedDelta = hasComparison
    ? contactedProviders - (comparison?.summary?.contactedProviders ?? 0)
    : null
  const trainedDelta = hasComparison
    ? trainedProviders - (comparison?.summary?.trainedProviders ?? 0)
    : null
  const canExport = rows.length > 0

  const formatGroupLabel = (value) =>
    String(value ?? '')
      .replace(/\bprimer\b/gi, '1er')
      .replace(/\bsegundo\b/gi, '2do')

  const extractGroupOrder = (value) => {
    const normalized = String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase()

    if (/\bprimer\b/.test(normalized)) {
      return 1
    }

    if (/\bsegundo\b/.test(normalized)) {
      return 2
    }

    const ordinalMatch = normalized.match(/\b(\d+)\s*(?:er|do|to|mo|vo|no)\b/)
    if (ordinalMatch) {
      return Number(ordinalMatch[1])
    }

    const numberMatch = normalized.match(/\bgrupo\s*(\d+)\b|\b(\d+)\s*grupo\b/)
    if (numberMatch) {
      return Number(numberMatch[1] ?? numberMatch[2])
    }

    return Number.POSITIVE_INFINITY
  }

  const sortedRows = [...rows].sort((a, b) => {
    const orderA = extractGroupOrder(a.group)
    const orderB = extractGroupOrder(b.group)
    if (orderA !== orderB) {
      return orderA - orderB
    }

    return formatGroupLabel(a.group).localeCompare(formatGroupLabel(b.group), 'es')
  })

  const sanitizeFilePart = (value) =>
    String(value || 'sin-stage')
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')

  const escapeCsvField = (value) => `"${String(value ?? '').replace(/"/g, '""')}"`

  const handleExportCsv = () => {
    const header = [
      'Grupo',
      'Total',
      'Contactados (incluye capacitados)',
      'Capacitados',
      'Faltan por contactar',
      '% Contactados',
      '% Capacitados',
    ]

    const dataRows = sortedRows.map((row) => [
      formatGroupLabel(row.group),
      row.total,
      row.contacted,
      row.trained,
      row.pending,
      `${row.contactRate.toFixed(1)}%`,
      `${row.trainedRate.toFixed(1)}%`,
    ])

    const totalRow = [
      'TOTAL GENERAL',
      totalProviders,
      contactedProviders,
      trainedProviders,
      pendingProviders,
      `${contactRate.toFixed(1)}%`,
      `${trainedRate.toFixed(1)}%`,
    ]

    const allRows = [header, ...dataRows, totalRow]
    const csvContent = allRows.map((line) => line.map(escapeCsvField).join(';')).join('\n')
    const blob = new Blob([`\uFEFF${csvContent}`], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    const stagePart = sanitizeFilePart(contactStage)
    const trainedPart = sanitizeFilePart(trainedStage)

    link.href = url
    link.download = `contacto-capacitacion-por-grupo-${stagePart}-${trainedPart}.csv`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  return (
    <article className="group-contact-table">
      <header className="group-contact-table__header">
        <h3>Contacto y capacitacion por grupo</h3>
        <div className="group-contact-table__actions">
          <span>Contacto: {contactStage || 'Sin definir'}</span>
          <span>Capacitado: {trainedStage || 'Sin definir'}</span>
          {comparisonDayLabel ? <span>Vs previo: {comparisonDayLabel}</span> : null}
          <button
            type="button"
            className="group-contact-table__export-btn"
            onClick={handleExportCsv}
            disabled={!canExport}
          >
            Exportar CSV
          </button>
        </div>
      </header>

      {rows.length ? (
        <div className="group-contact-table__scroll">
          <div className="group-contact-table__body">
            <div className="group-contact-table__row group-contact-table__row--head">
              <span>Grupo</span>
              <span>Total</span>
              <span>Contactados*</span>
              <span>Capacitados</span>
              <span>Faltan</span>
              <span>% Cto</span>
              <span>% Cap</span>
            </div>

            {sortedRows.map((row) => {
              const previousRow = comparisonRowsByGroup.get(normalizeGroupKey(row.group))
              const rowContactedDelta = hasComparison ? row.contacted - (previousRow?.contacted ?? 0) : null
              const rowTrainedDelta = hasComparison ? row.trained - (previousRow?.trained ?? 0) : null

              return (
                <div key={row.group} className="group-contact-table__row">
                  <span title={row.group}>{formatGroupLabel(row.group)}</span>
                  <span>{row.total}</span>
                  <span>{renderMetricWithDelta(row.contacted, rowContactedDelta, hasComparison)}</span>
                  <span>{renderMetricWithDelta(row.trained, rowTrainedDelta, hasComparison)}</span>
                  <span>{row.pending}</span>
                  <span>{row.contactRate.toFixed(1)}%</span>
                  <span>{row.trainedRate.toFixed(1)}%</span>
                </div>
              )
            })}

            <div className="group-contact-table__row group-contact-table__row--total">
              <span>TOTAL GENERAL</span>
              <span>{totalProviders}</span>
              <span>{renderMetricWithDelta(contactedProviders, contactedDelta, hasComparison)}</span>
              <span>{renderMetricWithDelta(trainedProviders, trainedDelta, hasComparison)}</span>
              <span>{pendingProviders}</span>
              <span>{contactRate.toFixed(1)}%</span>
              <span>{trainedRate.toFixed(1)}%</span>
            </div>
          </div>
        </div>
      ) : (
        <p className="group-contact-table__empty">
          No hay datos suficientes. Verifica el mapeo de Grupo, Etapa e ID Proveedor.
        </p>
      )}

      {rows.length ? (
        <p className="group-contact-table__note">
          * Contactados incluye proveedores capacitados. Enrolados se calcula aparte con su columna, Piloto y etapa Facturando.
        </p>
      ) : null}
    </article>
  )
}

export default GroupContactTable
