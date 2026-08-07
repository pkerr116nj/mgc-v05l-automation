import { contextBridge, ipcRenderer } from "electron";
import type { ResearchReadModelName, ResearchReadModelResult } from "./shared/researchControlCenter";

const researchControlCenter = {
  getReadModel: (name: ResearchReadModelName): Promise<ResearchReadModelResult> =>
    ipcRenderer.invoke("research-control-center:get-read-model", name),
};

contextBridge.exposeInMainWorld("researchControlCenter", researchControlCenter);

export type ResearchControlCenterApi = typeof researchControlCenter;
