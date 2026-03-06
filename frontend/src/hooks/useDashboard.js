import { useState, useCallback } from "react";
import { processData } from "../api";
import { buildPayload } from "../services/dashboardService";

/**
 * Custom hook for managing Dashboard state and operations.
 */
export function useDashboard() {
  const [report, setReport] = useState(null);
  const [rawReport, setRawReport] = useState(null);
  const [cleanedReport, setCleanedReport] = useState(null);
  const [rawData, setRawData] = useState([]);
  const [cleanedData, setCleanedData] = useState([]);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState(null);

  const executeAnalysis = useCallback(async (formState) => {
    setError(null);
    setProcessing(true);
    setReport(null);
    setRawReport(null);
    setCleanedReport(null);
    setRawData([]);
    setCleanedData([]);

    try {
      const payload = buildPayload(formState);
      const data = await processData(payload);

      setReport(data.report);
      setRawReport(data.raw_report);
      setCleanedReport(data.cleaned_report);

      // Get raw data (before cleaning)
      const rawList = data.raw_data?.data || data.raw_data?.properties || [];
      setRawData(Array.isArray(rawList) ? rawList : []);

      // Get cleaned data (after remediation)
      const cleanedList =
        data.cleaned_data?.data || data.cleaned_data?.properties || [];
      setCleanedData(Array.isArray(cleanedList) ? cleanedList : []);

      return data;
    } catch (err) {
      setError(err.message);
      throw err;
    } finally {
      setProcessing(false);
    }
  }, []);

  const resetDashboard = useCallback(() => {
    setReport(null);
    setRawReport(null);
    setCleanedReport(null);
    setRawData([]);
    setCleanedData([]);
    setError(null);
  }, []);

  return {
    report,
    rawReport,
    cleanedReport,
    rawData,
    cleanedData,
    processing,
    error,
    executeAnalysis,
    resetDashboard,
  };
}
