import './CitationAnalysisCard.css'

const formatGroupLabel = (value) =>
  String(value ?? '')
    .replace(/\bprimer\b/gi, '1er')
    .replace(/\bsegundo\b/gi, '2do')

function CitationList({
  title,
  rows,
  total,
  showTotal = false,
  totalLabel = 'Total',
  formatLabel = (label) => label,
}) {
  const rowsTotal = rows.reduce((accumulator, row) => accumulator + row.count, 0)

  return (
    <section className="citation-analysis-card__section">
      <h4>{title}</h4>
      {rows.length ? (
        <div className="citation-analysis-card__list">
          {rows.map((row) => {
            const rate = total ? (row.count / total) * 100 : 0
            return (
              <div key={row.label} className="citation-analysis-card__row">
                <span title={formatLabel(row.label)}>{formatLabel(row.label)}</span>
                <div className="citation-analysis-card__metric">
                  <strong>{row.count}</strong>
                  <small>{rate.toFixed(1)}%</small>
                </div>
              </div>
            )
          })}
          {showTotal ? (
            <div className="citation-analysis-card__row citation-analysis-card__row--total">
              <span>{totalLabel}</span>
              <div className="citation-analysis-card__metric">
                <strong>{rowsTotal}</strong>
                <small>{total ? ((rowsTotal / total) * 100).toFixed(1) : '0.0'}%</small>
              </div>
            </div>
          ) : null}
        </div>
      ) : (
        <p className="citation-analysis-card__empty">Sin datos.</p>
      )}
    </section>
  )
}

function CitationTrainingDayBars({ title, rows, total, totalLabel = 'Total' }) {
  const maxCount = Math.max(...rows.map((row) => row.count), 1)
  const rowsTotal = rows.reduce((accumulator, row) => accumulator + row.count, 0)

  return (
    <section className="citation-analysis-card__section citation-analysis-card__section--bars">
      <h4>{title}</h4>
      {rows.length ? (
        <div className="citation-analysis-card__bars">
          {rows.map((row) => {
            const rate = total ? (row.count / total) * 100 : 0

            return (
              <div key={row.label} className="citation-analysis-card__bar-row">
                <span className="citation-analysis-card__bar-label" title={row.label}>
                  {row.label}
                </span>
                <div className="citation-analysis-card__bar-track">
                  <div
                    className="citation-analysis-card__bar-fill"
                    style={{ width: `${(row.count / maxCount) * 100}%` }}
                  />
                </div>
                <div className="citation-analysis-card__metric">
                  <strong>{row.count}</strong>
                  <small>{rate.toFixed(1)}%</small>
                </div>
              </div>
            )
          })}

          <div className="citation-analysis-card__row citation-analysis-card__row--total">
            <span>{totalLabel}</span>
            <div className="citation-analysis-card__metric">
              <strong>{rowsTotal}</strong>
              <small>{total ? ((rowsTotal / total) * 100).toFixed(1) : '0.0'}%</small>
            </div>
          </div>
        </div>
      ) : (
        <p className="citation-analysis-card__empty">Sin datos.</p>
      )}
    </section>
  )
}

function CitationAnalysisCard({ data }) {
  const showMissingTrainingDay = data.trainedWithoutTrainingDay > 0
  const showMultipleTrainingDays = data.providersWithMultipleTrainingDays > 0

  return (
    <article className="citation-analysis-card">
      <header className="citation-analysis-card__header">
        <h3>Citacion de capacitacion (Dia de Citacion)</h3>
        <div className="citation-analysis-card__badges">
          <span>Total de agendas en proveedores: {data.totalAppointments}</span>
          <span>Proveedores citados (todas las etapas): {data.providersWithCitation}</span>
          <span>
            Agendados y citados{data.contactStageLabel ? ` (${data.contactStageLabel})` : ''}:{' '}
            {data.providersScheduledAndCited}
          </span>
          <span>Capacitados (etapa actual): {data.trainedByStage}</span>
          <span>Cobertura etapa actual: {data.stageCoverageRate.toFixed(1)}%</span>
          <span>Capacitados con Dia de Capacitacion: {data.trainedByTrainingDay}</span>
          <span>Cobertura con Dia de Capacitacion: {data.coverageRate.toFixed(1)}%</span>
          {showMissingTrainingDay ? (
            <span>Capacitados sin Dia de Capacitacion: {data.trainedWithoutTrainingDay}</span>
          ) : null}
          {showMultipleTrainingDays ? (
            <span>Capacitados con multiples dias: {data.providersWithMultipleTrainingDays}</span>
          ) : null}
          <span>Dias con capacitacion: {data.trainingDaysCount}</span>
        </div>
      </header>

      <p className="citation-analysis-card__summary">
        Dia de Citacion mide agendas. Dia de Capacitacion reconstruye en que fechas se capacito y cuantos proveedores fueron.
      </p>

      <div className="citation-analysis-card__grid">
        <CitationList
          title="Citados por grupo"
          rows={data.byGroup}
          total={data.providersWithCitation}
          formatLabel={formatGroupLabel}
        />
        <CitationList
          title="Citados por etapa"
          rows={data.byStage}
          total={data.providersWithCitation}
        />
        <CitationTrainingDayBars
          title="Capacitados por dia (Dia de Capacitacion)"
          rows={data.byTrainingDay}
          total={data.totalTrainingAttendances}
          totalLabel="Total registros"
        />
      </div>
    </article>
  )
}

export default CitationAnalysisCard
