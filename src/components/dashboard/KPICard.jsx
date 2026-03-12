import './KPICard.css'

function KPICard({ title, value, trend, description }) {
  const trendClass = trend && trend.startsWith('-') ? 'negative' : 'positive'

  return (
    <article className="kpi-card">
      <header className="kpi-card__header">
        <span className="kpi-card__title">{title}</span>
        {trend ? <span className={`kpi-card__trend ${trendClass}`}>{trend}</span> : null}
      </header>
      <p className="kpi-card__value">{value}</p>
      {description ? <p className="kpi-card__description">{description}</p> : null}
    </article>
  )
}

export default KPICard
