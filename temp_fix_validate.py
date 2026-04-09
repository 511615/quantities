from pathlib import Path
import re
path = Path('apps/web/src/features/dataset-browser/DatasetRequestDrawer.tsx')
text = path.read_text(encoding='utf-8')
new_validate = '''  const validate = () => {
    if (!form.requestName.trim()) {
      return " Please enter a request name.\;
 }
 if (!form.startDate || !form.endDate) {
 return \Please select the date range.\;
 }
 if (!form.frequency) {
 return \Please choose a frequency.\;
 }
 if (domainSelections.some((entry) => !entry.sourceVendor)) {
 return \Select a vendor for each domain.\;
 }
 if (domainSelections.some((entry) => !entry.dataDomain)) {
 return \Select a data domain for each entry.\;
 }
 if (form.selectionMode === \manual_list\ && splitCsv(form.symbols).length === 0) {
 return \Provide at least one symbol in the manual list.\;
 }
 if (form.selectionMode !== \manual_list\ && !form.symbolCount.trim()) {
 return \Specify the symbol count or toggle manual mode.\;
 }
 if (domainSelections.length > 1 && !domainSelections.some((entry) => entry.dataDomain === \market\)) {
 return \Multi-domain requests must include the market domain.\;
 }
 return null;
 };'''
text = re.sub(r' const validate = \(\) => \{[\s\S]*? \};', new_validate, text, count=1)
path.write_text(text, encoding='utf-8')
