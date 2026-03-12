import './RescuedAnalysisCard.css'

const formatGroupLabel = (value) =>
  String(value ?? '')
    .replace(/\bprimer\b/gi, '1er')
    .replace(/\bsegundo\b/gi, '2do')

function RescuedAnalysisList({ title, rows, totalRescued, formatLabel = (label) => label }) {
  return (
    <section className="rescued-analysis-card__section">
      <h4>{title}</h4>
      {rows.length ? (
        <div className="rescued-analysis-card__list">
          {rows.map((row) => {
            const rate = totalRescued ? (row.count / totalRescued) * 100 : 0

            return (
              <div key={row.label} className="rescued-analysis-card__row">
                <span title={formatLabel(row.label)}>{formatLabel(row.label)}</span>
                <div className="rescued-analysis-card__metric">
                  <strong>{row.count}</strong>
                  <small>{rate.toFixed(1)}%</small>
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <p className="rescued-analysis-card__empty">Sin datos.</p>
      )}
    </section>
  )
}

function RescuedAnalysisCard({ data }) {
  return (
    <article className="rescued-analysis-card">
      <header className="rescued-analysis-card__header">
        <h3>Analisis de rescatados por</h3>
        <div className="rescued-analysis-card__badges">
          <span>Total rescatados: {data.totalRescued}</span>
          <span>Agendados Giuliano: {data.scheduledByGiuliano}</span>
          <span>Cobertura: {data.rescueRate.toFixed(1)}%</span>
        </div>
      </header>

      <div className="rescued-analysis-card__grid">
        <RescuedAnalysisList
          title="Rescatados por grupo"
          rows={data.byGroup}
          totalRescued={data.totalRescued}
          formatLabel={formatGroupLabel}
        />
        <RescuedAnalysisList
          title="Rescatados por etapa"
          rows={data.byStage}
          totalRescued={data.totalRescued}
        />
        <RescuedAnalysisList
          title="Top rescatado por"
          rows={data.byRescuer}
          totalRescued={data.totalRescued}
        />
      </div>
    </article>
  )
}

export default RescuedAnalysisCard
