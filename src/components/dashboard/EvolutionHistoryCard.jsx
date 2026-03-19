import './EvolutionHistoryCard.css'

function formatSignedValue(value, suffix = '') {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return '--'
  }

  if (numeric === 0) {
    return `0${suffix}`
  }

  const sign = numeric > 0 ? '+' : ''
  return `${sign}${numeric}${suffix}`
}

function resolveDeltaTrained(orderedSnapshots, index) {
  if (!Array.isArray(orderedSnapshots) || index <= 0 || index >= orderedSnapshots.length) {
    return null
  }

  const previous = orderedSnapshots[index - 1]
  const current = orderedSnapshots[index]
  return Number(current?.trainedProviders ?? 0) - Number(previous?.trainedProviders ?? 0)
}

function resolveDeltaByKey(orderedSnapshots, index, key) {
  if (!Array.isArray(orderedSnapshots) || index <= 0 || index >= orderedSnapshots.length) {
    return null
  }

  const previous = orderedSnapshots[index - 1]
  const current = orderedSnapshots[index]
  return Number(current?.[key] ?? 0) - Number(previous?.[key] ?? 0)
}

function renderDeltaValue(delta) {
  if (delta === null) {
    return '--'
  }

  const numeric = Number(delta)
  const isPositive = Number.isFinite(numeric) && numeric > 0

  return (
    <span className={`evolution-history-card__delta ${isPositive ? 'evolution-history-card__delta--up' : ''}`}>
      <span>{formatSignedValue(numeric)}</span>
      {isPositive ? <i className="evolution-history-card__delta-arrow" aria-hidden="true" /> : null}
    </span>
  )
}

function renderBadgeDeltaValue(delta) {
  if (delta === null) {
    return '--'
  }

  const numeric = Number(delta)
  const isPositive = Number.isFinite(numeric) && numeric > 0

  return (
    <strong className={`evolution-history-card__badge-delta ${isPositive ? 'evolution-history-card__badge-delta--up' : ''}`}>
      {formatSignedValue(numeric)}
      {isPositive ? <i className="evolution-history-card__delta-arrow" aria-hidden="true" /> : null}
    </strong>
  )
}

function renderMetricWithDelta(value, delta) {
  const numericValue = Number(value)
  const numericDelta = Number(delta)
  const hasPositiveDelta = Number.isFinite(numericDelta) && numericDelta > 0
  const valueDisplay = Number.isFinite(numericValue) ? numericValue : 0

  return (
    <span className="evolution-history-card__metric">
      <span className="evolution-history-card__metric-value">{valueDisplay}</span>
      <span
        className={`evolution-history-card__metric-change ${hasPositiveDelta ? 'evolution-history-card__metric-change--up' : 'evolution-history-card__metric-change--empty'}`}
      >
        {hasPositiveDelta ? (
          <>
            <span>{formatSignedValue(numericDelta)}</span>
            <i className="evolution-history-card__delta-arrow" aria-hidden="true" />
          </>
        ) : (
          <span className="evolution-history-card__metric-change-placeholder">+0</span>
        )}
      </span>
    </span>
  )
}

function formatDayLabel(dayKey) {
  const [year, month, day] = String(dayKey).split('-')
  if (!year || !month || !day) {
    return String(dayKey)
  }

  return `${day}/${month}/${year}`
}

function resolveSegmentLabel(segment) {
  const normalized = String(segment ?? '').toLowerCase()
  if (normalized === 'legacy') {
    return 'Legacy'
  }
  if (normalized === 'official') {
    return 'Oficial'
  }

  return '--'
}

function normalizeSnapshotsForDisplay(sortedSnapshots) {
  const previousCitedBySegment = new Map()

  return sortedSnapshots.map((snapshot) => {
    const segmentKey = String(snapshot?.timelineSegment ?? 'unknown')
    const citedProviders = Number(snapshot?.citedProviders ?? 0)
    const previousCited = previousCitedBySegment.get(segmentKey)
    const normalizedCitedProviders =
      Number.isFinite(previousCited) && Number.isFinite(citedProviders)
        ? Math.max(previousCited, citedProviders)
        : Number.isFinite(citedProviders)
          ? citedProviders
          : 0

    previousCitedBySegment.set(segmentKey, normalizedCitedProviders)

    return {
      ...snapshot,
      citedProviders: normalizedCitedProviders,
    }
  })
}

function EvolutionHistoryCard({ snapshots, selectedDayKey, onClearHistory, onExportHistoryCsv }) {
  const orderedSnapshots = normalizeSnapshotsForDisplay(
    [...snapshots].sort((a, b) => a.dayKey.localeCompare(b.dayKey)),
  )
  const latestSnapshot = orderedSnapshots[orderedSnapshots.length - 1]
  const selectedSnapshotIndex = selectedDayKey
    ? orderedSnapshots.findIndex((snapshot) => snapshot.dayKey === selectedDayKey)
    : -1
  const deltaReferenceIndex =
    selectedSnapshotIndex >= 0 ? selectedSnapshotIndex : orderedSnapshots.length - 1
  const deltaReferenceSnapshot =
    deltaReferenceIndex >= 0 ? orderedSnapshots[deltaReferenceIndex] : null
  const referenceTrainedDelta = deltaReferenceSnapshot
    ? resolveDeltaTrained(orderedSnapshots, deltaReferenceIndex)
    : null
  const isUsingSelectedDay = selectedSnapshotIndex >= 0

  return (
    <article className="evolution-history-card">
      <header className="evolution-history-card__header">
        <h3>Evolutivo diario</h3>
        <div className="evolution-history-card__badges">
          <span>Dias guardados: {orderedSnapshots.length}</span>
          <span>Ultimo: {latestSnapshot ? formatDayLabel(latestSnapshot.dayKey) : 'Sin datos'}</span>
          <span>
            Delta capacitados{isUsingSelectedDay && deltaReferenceSnapshot
              ? ` (${formatDayLabel(deltaReferenceSnapshot.dayKey)})`
              : ''}: {renderBadgeDeltaValue(referenceTrainedDelta)}
          </span>
        </div>
      </header>

      <p className="evolution-history-card__summary">
        Se guarda un snapshot por dia para comparar evolucion de contactados, capacitados y rescatados.
      </p>

      {orderedSnapshots.length ? (
        <div className="evolution-history-card__table">
          <div className="evolution-history-card__row evolution-history-card__row--head">
            <span>Fecha</span>
            <span>Total</span>
            <span>Contactados</span>
            <span>Capacitados</span>
            <span>Rescatados</span>
            <span>Citados</span>
            <span>Segmento</span>
            <span>Delta cap.</span>
          </div>

          {orderedSnapshots.map((snapshot, index) => {
            const deltaTrained = resolveDeltaTrained(orderedSnapshots, index)
            const deltaContacted = resolveDeltaByKey(orderedSnapshots, index, 'contactedProviders')
            const deltaCited = resolveDeltaByKey(orderedSnapshots, index, 'citedProviders')

            return (
              <div key={snapshot.dayKey} className="evolution-history-card__row">
                <span>{formatDayLabel(snapshot.dayKey)}</span>
                <span>{snapshot.totalProviders}</span>
                <span>{renderMetricWithDelta(snapshot.contactedProviders, deltaContacted)}</span>
                <span>{renderMetricWithDelta(snapshot.trainedProviders, deltaTrained)}</span>
                <span>{snapshot.rescuedProviders}</span>
                <span>{renderMetricWithDelta(snapshot.citedProviders, deltaCited)}</span>
                <span>{resolveSegmentLabel(snapshot.timelineSegment)}</span>
                <span>{renderDeltaValue(deltaTrained)}</span>
              </div>
            )
          })}
        </div>
      ) : (
        <p className="evolution-history-card__empty">
          Carga un archivo y se empezara a guardar el historial diario automaticamente.
        </p>
      )}

      <footer className="evolution-history-card__footer">
        <button
          type="button"
          className="evolution-history-card__btn"
          onClick={onExportHistoryCsv}
          disabled={!orderedSnapshots.length}
        >
          Exportar historial CSV
        </button>
        <button
          type="button"
          className="evolution-history-card__btn evolution-history-card__btn--danger"
          onClick={onClearHistory}
          disabled={!orderedSnapshots.length}
        >
          Borrar historial
        </button>
      </footer>
    </article>
  )
}

export default EvolutionHistoryCard
