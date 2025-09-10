// constants.js
export const allJobTitles = {
    "R&D / Engineering": [
        { id: 'software', title: 'Software' },
        { id: 'hardware', title: 'Hardware' },
        { id: 'firmware', title: 'Firmware' },
        { id: 'devops', title: 'DevOps' },
        { id: 'production_engineering', title: 'Production Engineering' },
        { id: 'sre', title: 'SRE' },
        { id: 'qa', title: 'QA' },
        { id: 'automation', title: 'Automation' },
        { id: 'systems', title: 'Systems' },
        { id: 'cybersecurity', title: 'Cybersecurity' },
        { id: 'mechanical', title: 'Mechanical' },
        { id: 'security', title: 'Security' },
    ],
    "Data & Research": [
        { id: 'research', title: 'Research' },
        { id: 'data_engineering', title: 'Data Engineering' },
        { id: 'data_science', title: 'Data Science' },
        { id: 'data_analytics_bi', title: 'Data Analytics / BI' },
        { id: 'machine_learning_ai', title: 'Machine Learning / AI' },
    ],
    "Product & Business": [
        { id: 'product_management', title: 'Product Management' },
        { id: 'project_management', title: 'Project Management' },
        { id: 'program_management', title: 'Program Management' },
        { id: 'business_analysis', title: 'Business Analysis' },
        { id: 'business_development', title: 'Business Development' },
        { id: 'operations', title: 'Operations' },
        { id: 'customer_success_support', title: 'Customer Success / Support' },
    ],
    "Sales & Corporate": [
        { id: 'sales', title: 'Sales' },
        { id: 'marketing', title: 'Marketing' },
        { id: 'finance', title: 'Finance' },
        { id: 'legal', title: 'Legal' },
        { id: 'hr_people', title: 'HR / People' },
        { id: 'it_sysadmin', title: 'IT / SysAdmin' },
        { id: 'procurement_supply_chain', title: 'Procurement / Supply Chain' },
    ],
    "Design": [
        { id: 'product_design', title: 'Product Design' },
        { id: 'ux_ui_design', title: 'UX/UI Design' },
        { id: 'visual_creative_design', title: 'Visual / Creative Design' },
    ]
};

export const flattenedJobTitles = Object.values(allJobTitles).flat();