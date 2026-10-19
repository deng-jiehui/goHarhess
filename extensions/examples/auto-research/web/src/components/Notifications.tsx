import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import type { ReactNode } from "react";
import { Icon } from "./Icon";

const NoticeContext = createContext<(message: string) => void>(() => undefined);
export const useNotice = () => useContext(NoticeContext);

export function Notifications({ children }: { children: ReactNode }) {
  const [notice, setNotice] = useState<{ text: string; id: number } | null>(
    null,
  );
  const notify = useCallback(
    (text: string) => setNotice({ text, id: Date.now() }),
    [],
  );
  useEffect(() => {
    if (notice) {
      const timer = setTimeout(() => setNotice(null), 4500);
      return () => clearTimeout(timer);
    }
  }, [notice]);
  return (
    <NoticeContext.Provider value={notify}>
      {children}
      {notice && (
        <div className="toast" role="status">
          <Icon name="check" size={18} />
          {notice.text}
        </div>
      )}
    </NoticeContext.Provider>
  );
}
