import { ReactNode } from "react";

export function SectionCard({ title, subtitle, actions, children }: { title: string; subtitle?: string; actions?: ReactNode; children: ReactNode }) {
  return (
    <section className="panel stack-gap-sm">
      <header className="panel-head">
        <div>
          <h3 className="panel-title">{title}</h3>
          {subtitle ? <p className="panel-subtitle">{subtitle}</p> : null}
        </div>
        {actions ? <div className="panel-actions">{actions}</div> : null}
      </header>
      {children}
    </section>
  );
}
