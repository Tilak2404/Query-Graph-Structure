import { useState } from "react";
import GraphView from "./GraphView";
import ChatPanel from "./ChatPanel";
import "./App.css";

export default function App() {
  const [graphFocus, setGraphFocus] = useState(null);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Graph Query System</h1>
        <p>Explore O2C data - ask questions in natural language</p>
      </header>
      <main className="app-main">
        <section className="graph-section">
          <GraphView graphFocus={graphFocus} onClearGraphFocus={() => setGraphFocus(null)} />
        </section>
        <aside className="chat-section">
          <ChatPanel onGraphFocus={setGraphFocus} />
        </aside>
      </main>
    </div>
  );
}
