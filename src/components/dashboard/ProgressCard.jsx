import './ProgressCard.css'

function normalizeItem(item) {
  const safeTarget = Number(item?.targetValue) > 0 ? Number(item.targetValue) : 0
  const safeCurrent = Number(item?.currentValue) > 0 ? Number(item.currentValue) : 0
  const progress = safeTarget ? Math.min(Math.round((safeCurrent / safeTarget) * 100), 100) : 0

  return {
    title: item?.title || 'Cobertura',
    safeCurrent,
    safeTarget,
    progress,
  }
}

function ProgressCard({ title, currentValue, targetValue, items }) {
  const normalizedItems =
    Array.isArray(items) && items.length
      ? items.map((item) => normalizeItem(item))
      : [normalizeItem({ title, currentValue, targetValue })]

  const isMulti = normalizedItems.length > 1

  return (
    <article className="progress-card">
      {isMulti ? <h3 className="progress-card__title">{title}</h3> : null}

      <div className="progress-card__items">
        {normalizedItems.map((item, index) => (
          <div key={`${item.title}-${index}`} className="progress-card__item">
            <header className="progress-card__header">
              <h3>{item.title}</h3>
              <span>{item.progress}%</span>
            </header>
            <div className="progress-card__track" aria-label={`${item.title} progress`}>
              <div className="progress-card__fill" style={{ width: `${item.progress}%` }} />
            </div>
            <p className="progress-card__meta">
              {item.safeCurrent} / {item.safeTarget}
            </p>
          </div>
        ))}
      </div>
    </article>
  )
}

export default ProgressCard
