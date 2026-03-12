function SimpleBarChart({ title, data, maxValue, formatValue }) {
  const resolvedMaxValue = maxValue ?? Math.max(...data.map((item) => item.value), 1)

  return (
    <article className="simple-bar-chart">
      <h3>{title}</h3>
      <div className="simple-bar-chart__list">
        {data.map((item) => (
          <div key={item.label} className="simple-bar-chart__item">
            <span className="simple-bar-chart__label">{item.label}</span>
            <div className="simple-bar-chart__bar-wrap">
              <div
                className="simple-bar-chart__bar"
                style={{
                  width: `${(item.value / resolvedMaxValue) * 100}%`,
                  backgroundColor: item.color || '#2563eb',
                }}
              />
            </div>
            <span className="simple-bar-chart__value">
              {formatValue ? formatValue(item.value, item) : item.value}
            </span>
          </div>
        ))}
      </div>
    </article>
  )
}

export default SimpleBarChart
