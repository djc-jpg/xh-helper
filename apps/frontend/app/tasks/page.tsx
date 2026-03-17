import { redirect } from "next/navigation";

export default function LegacyTasksPage() {
  redirect("/runs");
}
