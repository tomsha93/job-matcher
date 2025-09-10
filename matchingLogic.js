// This file contains all the business logic for matching users to jobs.

/**
 * Normalizes a raw job object from BigQuery into a clean, predictable format.
 * This is the "preprocessing" step.
 * @param {object} jobFromBigQuery - A raw row from the BigQuery table.
 * @returns {object} A normalized job object.
 */
function normalizeJob(jobFromBigQuery) {
    const normalized = {};

    // Use a unique URL as the job's ID
    normalized.id = jobFromBigQuery.source_url;
    normalized.title = jobFromBigQuery.title;
    normalized.url = jobFromBigQuery.url;

    // Normalize Locations: Convert "Tel Aviv, Remote" into ['tel_aviv', 'remote']
    if (jobFromBigQuery.locations) {
        normalized.locations = jobFromBigQuery.locations
            .split(',')
            .map(loc => loc.trim().toLowerCase().replace(/\s+/g, '_'));
    } else {
        normalized.locations = [];
    }

    // Normalize Job Scope (Domains): Convert "Software" into ['software']
    if (jobFromBigQuery.job_scope) {
        normalized.domains = jobFromBigQuery.job_scope
            .split(',')
            .map(scope => scope.trim().toLowerCase().replace(/\s+/g, '_'));
    } else {
        normalized.domains = [];
    }
    
    // Normalize Experience Level: Convert strings like "2-4" or "5+" into a number range
    normalized.minExp = 0;
    normalized.maxExp = 99; // Represents infinity
    normalized.isStudentJob = false;
    if (jobFromBigQuery.experience_level) {
        const expStr = jobFromBigQuery.experience_level;
        if (expStr.includes('B.Sc') || expStr.includes('M.Sc')) {
            normalized.isStudentJob = true;
        }
        
        const rangeMatch = expStr.match(/(\d+)\s*-\s*(\d+)/); // Matches "2-4"
        const singlePlusMatch = expStr.match(/(\d+)\+/); // Matches "5+"
        const singleDigitMatch = expStr.match(/\b(\d)\b/); // Matches "0" or "5"

        if (rangeMatch) {
            normalized.minExp = parseInt(rangeMatch[1], 10);
            normalized.maxExp = parseInt(rangeMatch[2], 10);
        } else if (singlePlusMatch) {
            normalized.minExp = parseInt(singlePlusMatch[1], 10);
        } else if (singleDigitMatch) {
            normalized.minExp = parseInt(singleDigitMatch[1], 10);
            normalized.maxExp = parseInt(singleDigitMatch[1], 10);
        }
    }

    // Normalize Leadership Level: Convert "Team Lead" into "team_lead"
    if (jobFromBigQuery.leadership_level) {
        normalized.leadershipLevel = jobFromBigQuery.leadership_level.trim().toLowerCase().replace(/\s+/g, '_');
    } else {
        normalized.leadershipLevel = null;
    }

    return normalized;
}

/**
 * Checks if a user's experience preferences match a job's requirements.
 * @param {object} userPrefs - The user's preferences from Firestore.
 * @param {object} normalizedJob - The cleaned job data.
 * @returns {boolean} True if there is a match, false otherwise.
 */
function matchesExperience(userPrefs, normalizedJob) {
    // Handle student case
    if (userPrefs.status === 'student_position') {
        return normalizedJob.isStudentJob;
    }
    // Handle experience range overlap
    // The user's range must overlap with the job's required range
    return userPrefs.minExperience <= normalizedJob.maxExp && userPrefs.maxExperience >= normalizedJob.minExp;
}

/**
 * Checks if a user's domain preferences match a job's scope.
 * @param {string[]} userDomains - Array of user's preferred domains.
 * @param {string[]} jobDomains - Array of job's domains.
 * @returns {boolean} True if there is a match, false otherwise.
 */
function matchesDomains(userDomains, jobDomains) {
    if (!userDomains || userDomains.length === 0) return true; // User is open to all domains
    if (!jobDomains || jobDomains.length === 0) return true; // Job is open to all domains
    // Check if any of the user's preferred domains are in the job's domain list
    return userDomains.some(userDomain => jobDomains.includes(userDomain));
}

/**
 * Checks if a user's location preferences match a job's locations.
 * @param {string[]} userLocations - Array of user's preferred locations.
 * @param {string[]} jobLocations - Array of job's locations.
 * @returns {boolean} True if there is a match, false otherwise.
 */
function matchesLocation(userLocations, jobLocations) {
    if (!userLocations || userLocations.length === 0) return true;
    if (!jobLocations || jobLocations.length === 0) return true;
    
    // If the user or job wants 'remote', it's a potential match.
    // A more complex rule could be added later if needed.
    if (userLocations.includes('remote') || jobLocations.includes('remote')) {
        return true;
    }
    
    // Check if any of the user's preferred locations are in the job's location list
    return userLocations.some(userLoc => jobLocations.includes(userLoc));
}

/**
 * Checks if a user's management preferences match a job's leadership level.
 * @param {object} userPrefs - The user's preferences from Firestore.
 * @param {object} normalizedJob - The cleaned job data.
 * @returns {boolean} True if there is a match, false otherwise.
 */
function matchesLeadership(userPrefs, normalizedJob) {
    const interest = userPrefs.managementInterest;
    
    if (!interest) { // For users who weren't asked (students, no-experience)
        return !normalizedJob.leadershipLevel; // Only match non-management jobs
    }

    if (interest === 'no_management') {
        return !normalizedJob.leadershipLevel; // Only match if the job is not a leadership role
    }
    if (interest === 'management_only') {
        // Must be a leadership role AND the level must be one the user wants
        if (!normalizedJob.leadershipLevel) return false;
        return userPrefs.managementLevel.includes(normalizedJob.leadershipLevel);
    }
    if (interest === 'management_and_individual') {
        // Can be a non-leadership role OR a leadership role the user wants
        if (!normalizedJob.leadershipLevel) return true;
        return userPrefs.managementLevel.includes(normalizedJob.leadershipLevel);
    }
    
    return true; // Default case
}


/**
 * The main function that checks if a user and a job are a match across all criteria.
 * @param {object} userPreferences - The user's full preference object from Firestore.
 * @param {object} jobFromBigQuery - A raw row from the BigQuery table.
 * @returns {boolean} True if it's a match, false otherwise.
 */
export function isMatch(userPreferences, jobFromBigQuery) {
    const job = normalizeJob(jobFromBigQuery);

    if (!matchesExperience(userPreferences, job)) return false;
    if (!matchesDomains(userPreferences.domains, job.domains)) return false;
    if (!matchesLocation(userPreferences.locations, job.locations)) return false;
    if (!matchesLeadership(userPreferences, job)) return false;
    
    // If all checks pass, it's a match!
    return true;
}