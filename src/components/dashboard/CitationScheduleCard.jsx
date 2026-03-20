import './CitationScheduleCard.css'

function formatDayLabel(dayKey) {
  const [year, month, day] = String(dayKey).split('-')
  if (!year || !month || !day) {
    return String(dayKey ?? '')
  }

  return `${day}/${month}/${year}`
}

function formatSignedValue(value) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) {
    return '--'
  }

  if (numeric === 0) {
    return '0'
  }

  return `${numeric > 0 ? '+' : ''}${numeric}`
}

function formatWeeklyDelta(delta) {
  const numeric = Number(delta)
  if (!Number.isFinite(numeric)) {
    return '--'
  }

  return `${formatSignedValue(numeric)} vs semana previa`
}

function CitationScheduleBars({ title, rows, emptyLabel }) {
  const maxAppointments = Math.max(...rows.map((row) => row.appointments), 1)

  return (
    <section className="citation-schedule-card__section">
      <h4>{title}</h4>
      {rows.length ? (
        <div className="citation-schedule-card__bars">
          {rows.map((row) => (
            <div key={row.label} className="citation-schedule-card__bar-row">
              <span className="citation-schedule-card__bar-label" title={row.label}>
                {row.label}
              </span>
              <div className="citation-schedule-card__bar-track">
                <div
                  className="citation-schedule-card__bar-fill"
                  style={{ width: `${(row.appointments / maxAppointments) * 100}%` }}
                />
              </div>
              <div className="citation-schedule-card__metric">
                <strong>{row.appointments}</strong>
                <small>{row.providers} prov. unicos</small>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p className="citation-schedule-card__empty">{emptyLabel}</p>
      )}
    </section>
  )
}

function CitationScheduleCard({ data }) {
  const dailyRows = [...(data?.appointmentsByDay ?? [])].slice(-12).reverse()
  const weeklyRows = [...(data?.appointmentsByWeek ?? [])].slice(-8).reverse()
  const latestDayLabel = data?.latestCitationDay
    ? `${data.latestCitationDay.label} (${data.latestCitationDay.appointments})`
    : 'Sin datos'
  const latestWeekLabel = data?.latestCitationWeek
    ? `${data.latestCitationWeek.label} (${data.latestCitationWeek.appointments})`
    : 'Sin datos'
  const weeklyDelta = formatWeeklyDelta(data?.weeklyAppointmentsDelta)
  const cutoffLabel = data?.citationCutoffDayKey
    ? formatDayLabel(data.citationCutoffDayKey)
    : 'Sin corte'

  return (
    <article className="citation-schedule-card">
      <header className="citation-schedule-card__header">
        <h3>Analisis de citados (Fecha de citacion)</h3>
        <div className="citation-schedule-card__badges">
          <span>Corte: {cutoffLabel}</span>
          <span>Agendas: {data?.totalAppointments ?? 0}</span>
          <span>Dias activos: {data?.activeCitationDays ?? 0}</span>
          <span>Semanas activas: {data?.activeCitationWeeks ?? 0}</span>
          <span>Ultimo dia: {latestDayLabel}</span>
          <span>Ultima semana: {latestWeekLabel}</span>
          <span>Delta semanal: {weeklyDelta}</span>
        </div>
      </header>

      <p className="citation-schedule-card__summary">
        Agendados diarios y semanales calculados desde la columna Fecha de citacion.
      </p>

      <div className="citation-schedule-card__grid">
        <CitationScheduleBars
          title="Agendados diarios (ultimos 12 dias con cita)"
          rows={dailyRows}
          emptyLabel="No hay fechas de citacion validas para mostrar."
        />
        <CitationScheduleBars
          title="Agendados semanales (ultimas 8 semanas)"
          rows={weeklyRows}
          emptyLabel="No hay semanas con citaciones para mostrar."
        />
      </div>
    </article>
  )
}

export default CitationScheduleCard
