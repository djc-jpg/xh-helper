export function SkeletonBlock({ height = 16 }: { height?: number }) {
  return <div className="skeleton" style={{ height }} aria-hidden="true" />;
}

export function PageSkeleton() {
  return (
    <div className="panel stack-gap-md">
      <SkeletonBlock height={24} />
      <SkeletonBlock height={16} />
      <SkeletonBlock height={16} />
      <SkeletonBlock height={200} />
    </div>
  );
}
