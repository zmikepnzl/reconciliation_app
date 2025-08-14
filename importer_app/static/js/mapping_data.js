document.addEventListener('DOMContentLoaded', () => {
    // --- CONSTANTS AND VARIABLES ---
    const API_BASE_URL = '/admin/mapping_rules/api/metadata';
    const API_SAVE_SAMPLE_URL = '/admin/mapping_rules/mappings/lines/save_sample_data';
    let allRules = [];
    let newRuleCounter = 0;
    let ruleToDeleteId = null;

    const ALL_ROLES = ["header", "item", "line", "account", "ignore"];
    const ALL_SOURCE_TYPES = ["CSV", "CSV Formula", "Text Override", "Link"];
    const ROLE_DESTINATION_FIELDS = {
        "header": [{"value":"id","text":"ID"},{"value":"supplier_id","text":"Supplier ID"},{"value":"invoice_number","text":"Invoice Number"},{"value":"invoice_date","text":"Invoice Date"},{"value":"due_date","text":"Due Date"},{"value":"total_amount","text":"Total Amount"},{"value":"tax_amount","text":"Tax Amount"},{"value":"account_number_id","text":"Account Number ID"},{"value":"notes","text":"Notes"}],
        "item": [{"value":"id","text":"ID"},{"value":"supplier_id","text":"Supplier ID"},{"value":"billing_reference","text":"Billing Reference"},{"value":"account_number_id","text":"Account Number ID"},{"value":"audit_date","text":"Audit Date"},{"value":"contract_start_date","text":"Contract Start Date"},{"value":"contract_end_date","text":"Contract End Date"},{"value":"contract_term_in_months","text":"Contract Term In Months"},{"value":"review_flag","text":"Review Flag"},{"value":"notes","text":"Notes"}],
        "line": [{"value":"id","text":"ID"},{"value":"invoice_header_id","text":"Invoice Header ID"},{"value":"item_id","text":"Item ID"},{"value":"unique_reference","text":"Unique Reference"},{"value":"description","text":"Description"},{"value":"quantity","text":"Quantity"},{"value":"unit_price","text":"Unit Price"},{"value":"total_amount","text":"Total Amount"},{"value":"start_date","text":"Start Date"},{"value":"end_date","text":"End Date"}],
        "account": [{"value":"id","text":"ID"},{"value":"account_number","text":"Account Number"},{"value":"supplier_id","text":"Supplier ID"}],
        "ignore": [{"value":"","text":"N/A (No Destination)"}]
    };
    const FIELD_ROLE_ORDER = { "header": 1, "item": 2, "line": 3, "account": 4, "ignore": 5 };
    const SUPPLIER_FORMULA_FIELDS = [{ value: "Supplier Name", text: "Supplier Name" }, { value: "Supplier Short Name", text: "Supplier Short Name" }];

    // Functions to be used across pages
    window.showMessage = function(message, type = 'success') {
        const container = document.querySelector('.page-container');
        if (!container) return;
        const messageDiv = document.createElement('div');
        messageDiv.className = `alert alert-${type}`;
        messageDiv.textContent = message;
        container.prepend(messageDiv);
        setTimeout(() => messageDiv.remove(), 5000);
    }
    
    window.populateSelect = function(selectElement, options, selectedValues = []) {
        selectElement.innerHTML = '';
        options.forEach(optionData => {
            const option = new Option(optionData.text, optionData.value);
            if (selectElement.multiple) {
                if (Array.isArray(selectedValues) && selectedValues.includes(optionData.value)) {
                    option.selected = true;
                }
            } else if (String(optionData.value) === String(selectedValues)) {
                option.selected = true;
            }
            selectElement.add(option);
        });
    }

    window.populateDestinationFields = function(selectElement, fieldRole, currentDestinationField = null) {
        const fields = ROLE_DESTINATION_FIELDS[fieldRole] || [];
        populateSelect(selectElement, fields, currentDestinationField);
    }

    window.updateSourceDetailsDisplay = function(fieldRole, destinationField, sourceTypeSelect, sourceDetailsContainer, currentFormulaValue, currentLinkedRuleId, currentRow) {
        const selectedSourceTypes = Array.from(sourceTypeSelect.selectedOptions).map(option => option.value.toLowerCase());
        sourceDetailsContainer.innerHTML = '';
        if (selectedSourceTypes.includes('link')) {
            const linkSourceSelect = document.createElement('select');
            linkSourceSelect.className = 'link-source-select';
            populateLinkSourceDropdown(linkSourceSelect, selectedSourceTypes, currentLinkedRuleId, currentRow);
            sourceDetailsContainer.appendChild(linkSourceSelect);
        } else if (selectedSourceTypes.includes('csv formula')) {
            const formulaTextarea = document.createElement('textarea');
            formulaTextarea.className = 'formula-input';
            formulaTextarea.value = currentFormulaValue || '';
            formulaTextarea.placeholder = 'e.g., {Col1}-{Col2}';
            const formulaBuilderSelect = document.createElement('select');
            formulaBuilderSelect.className = 'formula-builder-select';
            formulaBuilderSelect.style.marginTop = '5px';
            populateFormulaBuilderDropdown(formulaBuilderSelect, formulaTextarea, currentRow);
            sourceDetailsContainer.appendChild(formulaTextarea);
            sourceDetailsContainer.appendChild(formulaBuilderSelect);
        } else if (selectedSourceTypes.includes('text override')) {
            const textInput = document.createElement('input');
            textInput.type = 'text';
            textInput.className = 'text-override-input';
            textInput.value = currentFormulaValue || '';
            textInput.placeholder = 'Enter static value';
            sourceDetailsContainer.appendChild(textInput);
        } else if (selectedSourceTypes.includes('csv')) {
            sourceDetailsContainer.innerHTML = '<span>CSV Column</span>';
        } else {
            sourceDetailsContainer.innerHTML = '<span>-</span>';
        }
    }

    window.populateFormulaBuilderDropdown = function(selectElement, formulaTextarea, currentRow) {
        selectElement.innerHTML = '<option value="">-- Insert Placeholder --</option>';
        const currentRuleId = currentRow ? currentRow.dataset.id : null;
        allRules.filter(r => String(r.id) !== String(currentRuleId)).forEach(rule => {
            selectElement.add(new Option(`Rule: ${rule.rule_name} (${rule.field_role}.${rule.destination_field})`, `{${rule.rule_name}}`));
        });
        SUPPLIER_FORMULA_FIELDS.forEach(field => {
            selectElement.add(new Option(`Supplier: ${field.text}`, `{${field.value}}`));
        });
        selectElement.addEventListener('change', (e) => {
            if (e.target.value) {
                formulaTextarea.value += e.target.value;
                e.target.selectedIndex = 0;
            }
        });
    }

    window.getRulePayload = function(row) {
        const isBaseRule = row.querySelector('input[data-field="is_base_rule"]').checked;
        const isRequiredInput = row.querySelector('input[data-field="is_required"]');
        const sourceTypeOptions = Array.from(row.querySelector('select[data-field="source_type_options"]').selectedOptions).map(option => option.value).join(',');
        let linkTableLookup = null, linkFieldLookup = null, formula_template = null, static_value = null;

        if (sourceTypeOptions.includes('Link')) {
            const linkSourceSelect = row.querySelector('.link-source-select');
            if (linkSourceSelect && linkSourceSelect.value) {
                const linkedRule = allRules.find(r => String(r.id) === linkSourceSelect.value);
                if (linkedRule) {
                    const tableMap = { "header": "supplier_invoice_headers", "item": "supplier_invoice_items", "line": "supplier_invoice_lines", "account": "supplier_account" };
                    linkTableLookup = tableMap[linkedRule.field_role] || null;
                    linkFieldLookup = linkedRule.destination_field;
                }
            }
        } else if (sourceTypeOptions.includes('CSV Formula')) {
            const formulaInput = row.querySelector('.formula-input');
            if (formulaInput) formula_template = formulaInput.value || null;
        } else if (sourceTypeOptions.includes('Text Override')) {
            const textInput = row.querySelector('.text-override-input');
            if (textInput) static_value = textInput.value || null;
        }
        
        return {
            id: row.dataset.id && !String(row.dataset.id).startsWith('new_') ? row.dataset.id : null,
            rule_name: row.querySelector('input[data-field="rule_name"]').value,
            field_role: row.querySelector('select[data-field="field_role"]').value,
            destination_field: row.querySelector('select[data-field="destination_field"]').value,
            source_type_options: sourceTypeOptions,
            rule_type: 'user_set',
            is_required: isBaseRule || (isRequiredInput ? isRequiredInput.checked : false),
            is_hidden: row.querySelector('input[data-field="is_hidden"]').checked,
            is_base_rule: isBaseRule,
            link_table_lookup: linkTableLookup,
            link_field_lookup: linkFieldLookup,
            formula_template: formula_template,
            static_value: static_value,
            default_transformation: row.querySelector('input[data-field="default_transformation"]').value || null
        };
    }

    window.createRuleRow = function(rule, isBase) {
        const tr = document.createElement('tr');
        tr.dataset.id = rule.id;
        const selectedSourceTypesArray = rule.source_type_options ? rule.source_type_options.split(',') : [];

        tr.innerHTML = `
            <td><input type="text" data-field="rule_name" value="${rule.rule_name || ''}"></td>
            <td><select data-field="field_role"></select></td>
            <td><select data-field="destination_field"></select></td>
            <td><select multiple size="3" data-field="source_type_options"></select></td>
            <td><div class="dynamic-source-input-container"></div></td>
            <td><input type="text" data-field="default_transformation" value="${rule.default_transformation || ''}" placeholder="e.g., totext"></td>
            <td><input type="checkbox" data-field="is_required" ${isBase || rule.is_required ? 'checked' : ''} ${isBase ? 'disabled' : ''}></td>
            <td><input type="checkbox" data-field="is_hidden" ${rule.is_hidden ? 'checked' : ''}></td>
            <td class="super-admin-col">
                <input type="checkbox" data-field="is_base_rule" ${rule.is_base_rule ? 'checked' : ''}>
            </td>
            <td><button class="delete-btn btn-danger">Delete</button></td>
        `;
        
        const fieldRoleSelect = tr.querySelector('[data-field="field_role"]');
        populateSelect(fieldRoleSelect, ALL_ROLES.map(r => ({value: r, text: r.charAt(0).toUpperCase() + r.slice(1)})), rule.field_role);
        
        const destFieldSelect = tr.querySelector('[data-field="destination_field"]');
        populateDestinationFields(destFieldSelect, rule.field_role, rule.destination_field);
        
        const sourceTypeSelect = tr.querySelector('[data-field="source_type_options"]');
        populateSelect(sourceTypeSelect, ALL_SOURCE_TYPES.map(t => ({value: t, text: t})), selectedSourceTypesArray);
        
        let currentLinkedRuleId = null;
        if (rule.link_table_lookup && rule.link_field_lookup) {
            const tableMap = { "supplier_invoice_headers": "header", "supplier_invoice_items": "item", "supplier_invoice_lines": "line", "supplier_account": "account" };
            const sourceRule = allRules.find(r => tableMap[rule.link_table_lookup] === r.field_role && r.destination_field === rule.link_field_lookup);
            if (sourceRule) currentLinkedRuleId = sourceRule.id;
        }

        const dynamicSourceContainer = tr.querySelector('.dynamic-source-input-container');
        updateSourceDetailsDisplay(rule.field_role, rule.destination_field, sourceTypeSelect, dynamicSourceContainer, rule.formula_template || rule.static_value, currentLinkedRuleId, tr);

        const handleSourceChange = () => {
            const currentRuleData = getRulePayload(tr);
            updateSourceDetailsDisplay(
                currentRuleData.field_role,
                currentRuleData.destination_field,
                sourceTypeSelect,
                dynamicSourceContainer,
                currentRuleData.formula_template || currentRuleData.static_value,
                currentRuleData.link_table_lookup && currentRuleData.link_field_lookup ?
                    (allRules.find(r => r.link_table_lookup === currentRuleData.link_table_lookup && r.link_field_lookup === currentRuleData.link_field_lookup) || {}).id :
                    null,
                tr
            );
        };
        
        fieldRoleSelect.addEventListener('change', () => { 
            populateDestinationFields(destFieldSelect, fieldRoleSelect.value); 
            handleSourceChange();
        });
        destFieldSelect.addEventListener('change', handleSourceChange);
        sourceTypeSelect.addEventListener('change', handleSourceChange);
        
        tr.querySelector('.delete-btn').addEventListener('click', () => {
            ruleToDeleteId = rule.id;
            showConfirmationModal();
        });
        return tr;
    }

    window.renderRules = function(rules) {
        const baseRulesTableBody = document.getElementById('base-rules-body');
        const additionalRulesTableBody = document.getElementById('additional-rules-body');
        if (!baseRulesTableBody || !additionalRulesTableBody) return;
        baseRulesTableBody.innerHTML = '';
        additionalRulesTableBody.innerHTML = '';
        rules.sort((a, b) => (FIELD_ROLE_ORDER[a.field_role] || 99) - (FIELD_ROLE_ORDER[b.field_role] || 99) || a.rule_name.localeCompare(b.rule_name));
        
        rules.forEach(rule => {
            const isBase = !!rule.is_base_rule;
            const row = createRuleRow(rule, isBase);
            if (isBase) {
                baseRulesTableBody.appendChild(row);
            } else {
                additionalRulesTableBody.appendChild(row);
            }
        });
        toggleBaseRuleEditability();
    }

    window.fetchRules = async function() {
        try {
            const response = await fetch(API_BASE_URL);
            if (!response.ok) throw new Error((await response.json()).message || 'Failed to fetch rules');
            allRules = await response.json();
            renderRules(allRules);
        } catch (error) {
            showMessage(`Error: ${error.message}`, 'error');
        }
    }

    window.saveAllChanges = async function() {
        const baseRulesTableBody = document.getElementById('base-rules-body');
        const additionalRulesTableBody = document.getElementById('additional-rules-body');
        if (!baseRulesTableBody || !additionalRulesTableBody) return;
        const allRows = [...baseRulesTableBody.querySelectorAll('tr'), ...additionalRulesTableBody.querySelectorAll('tr')];
        let successCount = 0;
        const promises = allRows.map(async (row) => {
            try {
                const payload = getRulePayload(row);
                const method = payload.id ? 'PUT' : 'POST';
                const url = payload.id ? `${API_BASE_URL}/${payload.id}` : API_BASE_URL;
                const response = await fetch(url, {
                    method: method,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                if (!response.ok) throw new Error((await response.json()).message || `Failed to save rule: ${payload.rule_name}`);
                successCount++;
            } catch (error) {
                showMessage(`Error saving "${row.querySelector('input[data-field="rule_name"]').value}": ${error.message}`, 'error');
            }
        });
        await Promise.all(promises);
        if (successCount > 0) showMessage(`Successfully saved ${successCount} rule(s).`, 'success');
        fetchRules();
    }

    window.addRow = function(isBase = false) {
        const newRule = { id: `new_${++newRuleCounter}`, rule_name: '', field_role: '', destination_field: '', source_type_options: '', rule_type: 'user_set', is_required: isBase, is_hidden: false, is_base_rule: isBase };
        const newRow = createRuleRow(newRule, isBase);
        const targetBody = isBase ? document.getElementById('base-rules-body') : document.getElementById('additional-rules-body');
        if (targetBody) {
            targetBody.appendChild(newRow);
        }
        toggleBaseRuleEditability();
    }

    window.deleteRule = async function() {
        if (!ruleToDeleteId) return;
        if (String(ruleToDeleteId).startsWith('new_')) {
            document.querySelector(`tr[data-id="${ruleToDeleteId}"]`)?.remove();
            showMessage('New unsaved rule removed.', 'success');
            hideConfirmationModal();
            return;
        }
        try {
            const response = await fetch(`${API_BASE_URL}/${ruleToDeleteId}`, { method: 'DELETE' });
            if (!response.ok) throw new Error((await response.json()).message || 'Failed to delete rule');
            showMessage('Rule deleted successfully!', 'success');
            fetchRules();
        } catch (error) {
            showMessage(`Error: ${error.message}`, 'error');
        }
        hideConfirmationModal();
    }

    window.showConfirmationModal = function() {
        const confirmationModal = document.getElementById('modal-confirm-delete');
        if (confirmationModal) confirmationModal.style.display = 'flex';
    }

    window.hideConfirmationModal = function() {
        const confirmationModal = document.getElementById('modal-confirm-delete');
        if (confirmationModal) confirmationModal.style.display = 'none';
        ruleToDeleteId = null;
    }

    window.populateLinkSourceDropdown = function(selectElement, selectedSourceTypes, currentLinkedRuleId, currentRow) {
        selectElement.innerHTML = '<option value="">-- Select Linked Rule --</option>';
        if (selectedSourceTypes.includes('link')) {
            const linkableRules = allRules.filter(r => {
                const isSelf = currentRow && String(r.id) === String(currentRow.dataset.id);
                const isNotLinkSource = !(r.source_type_options && r.source_type_options.includes('Link'));
                return !isSelf && isNotLinkSource;
            });
            populateSelect(selectElement, linkableRules.map(r => ({ value: r.id, text: `${r.rule_name} (${r.field_role}.${r.destination_field})` })), currentLinkedRuleId);
            if (linkableRules.length === 0) {
                selectElement.innerHTML += '<option value="" disabled>No available rules</option>';
            }
        }
    }

    window.toggleBaseRuleEditability = function() {
        const superAdminOverrideCheckbox = document.getElementById('chk-super-admin');
        const isOverrideActive = superAdminOverrideCheckbox ? superAdminOverrideCheckbox.checked : false;
        
        document.querySelectorAll('.super-admin-col').forEach(col => {
            col.style.display = isOverrideActive ? 'table-cell' : 'none';
        });
        
        const baseRulesTableBody = document.getElementById('base-rules-body');
        const additionalRulesTableBody = document.getElementById('additional-rules-body');
        const allRows = [...(baseRulesTableBody ? baseRulesTableBody.querySelectorAll('tr') : []), ...(additionalRulesTableBody ? additionalRulesTableBody.querySelectorAll('tr') : [])];

        allRows.forEach(row => {
            const isBaseRow = row.parentNode && row.parentNode.id === 'base-rules-body';
            row.querySelectorAll('input, select, button, textarea').forEach(el => {
                const field = el.dataset.field;
                if (field === 'is_base_rule') {
                    el.disabled = !isOverrideActive;
                } else if (field === 'is_required') {
                    el.disabled = isBaseRow;
                } else {
                    el.disabled = isBaseRow && !isOverrideActive;
                }
            });
        });
        const addNewBaseRuleBtn = document.getElementById('btn-add-base');
        if (addNewBaseRuleBtn) {
            addNewBaseRuleBtn.disabled = !isOverrideActive;
        }
    }
});