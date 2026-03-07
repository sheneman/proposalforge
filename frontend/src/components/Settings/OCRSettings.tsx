import ModelEndpointForm from './ModelEndpointForm';

export default function OCRSettings() {
  return (
    <ModelEndpointForm
      section="ocr"
      title="OCR / Document Processing"
      icon="bi-file-earmark-text"
      extraFields={[
        { key: 'ocr_method', label: 'OCR Method', options: ['dotsocr', 'pymupdf'] },
        { key: 'doc_workers', label: 'Concurrent Workers', type: 'number' },
      ]}
    />
  );
}
