type MetricGridProps = {
  items: Array<{ label: string; value: string }>;
};

export function MetricGrid({ items }: MetricGridProps) {
  return (
    <div className="metric-grid">
      {items.map((item) => (
        <div className="metric-tile" key={item.label}>
          <span>{item.label}</span>
          <strong>{item.value}</strong>
        </div>
      ))}
    </div>
  );
}
