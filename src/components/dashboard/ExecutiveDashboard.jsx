import KPICard from './KPICard'
import ProgressCard from './ProgressCard'
import SimpleBarChart from './SimpleBarChart'
import './ExecutiveDashboard.css'

const defaultKpis = [
  {
    title: 'Ingresos mensuales',
    value: '$124.500',
    trend: '+8.2%',
    description: 'vs. mes anterior',
  },
  {
    title: 'Clientes activos',
    value: '1.248',
    trend: '+3.4%',
    description: 'ultimos 30 dias',
  },
  {
    title: 'Tasa de churn',
    value: '2.1%',
    trend: '-0.5%',
    description: 'mejora sostenida',
  },
]

const defaultBarChartData = [
  { label: 'Norte', value: 45, color: '#1d4ed8' },
  { label: 'Centro', value: 72, color: '#0ea5e9' },
  { label: 'Sur', value: 58, color: '#14b8a6' },
  { label: 'Online', value: 90, color: '#22c55e' },
]

function ExecutiveDashboard({
  title = 'Executive Dashboard',
  subtitle = 'Resumen de indicadores clave de negocio',
  kpis = defaultKpis,
  progressCard = {
    title: 'Objetivo trimestral',
    currentValue: 74,
    targetValue: 100,
  },
  barChartTitle = 'Ventas por region',
  barChartData = defaultBarChartData,
  barChartMaxValue,
  barChartFormatValue,
  extraContent = null,
}) {
  const safeBarChartData = barChartData.length
    ? barChartData
    : [{ label: 'Sin datos', value: 0, color: '#94a3b8' }]

  return (
    <main className="executive-dashboard">
      <header className="executive-dashboard__header">
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </header>

      <section className="executive-dashboard__kpis">
        {kpis.map((kpi) => (
          <KPICard
            key={kpi.title}
            title={kpi.title}
            value={kpi.value}
            trend={kpi.trend}
            description={kpi.description}
          />
        ))}
      </section>

      <section className="executive-dashboard__content">
        <ProgressCard
          title={progressCard.title}
          currentValue={progressCard.currentValue}
          targetValue={progressCard.targetValue}
          items={progressCard.items}
        />
        <SimpleBarChart
          title={barChartTitle}
          data={safeBarChartData}
          maxValue={barChartMaxValue}
          formatValue={barChartFormatValue}
        />
      </section>

      {extraContent ? <section className="executive-dashboard__extra">{extraContent}</section> : null}
    </main>
  )
}

export default ExecutiveDashboard
