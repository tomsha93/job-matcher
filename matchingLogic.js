import { flattenedJobTitles } from './constants.js';

// This file contains all the business logic for matching users to jobs.

function normalizeJob(jobFromBigQuery) {
    const normalized = {};
    normalized.id = jobFromBigQuery.source_url;
    normalized.title = jobFromBigQuery.title;
    normalized.url = jobFromBigQuery.url;

    if (jobFromBigQuery.locations) {
        normalized.locations = jobFromBigQuery.locations
            .split(',')
            .map(loc => loc.trim().toLowerCase().replace(/\s+/g, '_'));
    } else {
        normalized.locations = [];
    }

    // --- FIX: This logic is now more robust ---
    // It finds the matching ID from our constants instead of just guessing.
    normalized.domains = [];
    if (jobFromBigQuery.job_scope) {
        const scopes = jobFromBigQuery.job_scope.split(',').map(s => s.trim());
        scopes.forEach(scope => {
            const foundJob = flattenedJobTitles.find(job => job.title.toLowerCase() === scope.toLowerCase());
            if (foundJob) {
                normalized.domains.push(foundJob.id);
            }
        });
    }

    normalized.minExp = 0;
    normalized.maxExp = 99;
    normalized.isStudentJob = false;
    if (jobFromBigQuery.experience_level) {
        const expStr = jobFromBigQuery.experience_level;
        if (expStr.includes('B.Sc') || expStr.includes('M.Sc')) {
            normalized.isStudentJob = true;
        }
        const rangeMatch = expStr.match(/(\d+)\s*-\s*(\d+)/);
        const singlePlusMatch = expStr.match(/(\d+)\+/);
        const singleDigitMatch = expStr.match(/\b(\d)\b/);

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

    if (jobFromBigQuery.leadership_level) {
        normalized.leadershipLevel = jobFromBigQuery.leadership_level.trim().toLowerCase().replace(/\s+/g, '_');
    } else {
        normalized.leadershipLevel = null;
    }

    return normalized;
}

function matchesExperience(userPrefs, normalizedJob) {
    if (userPrefs.status === 'student_position') {
        return normalizedJob.isStudentJob;
    }
    return userPrefs.minExperience <= normalizedJob.maxExp && userPrefs.maxExperience >= normalizedJob.minExp;
}

function matchesDomains(userDomains, jobDomains) {
    if (!userDomains || userDomains.length === 0) return true;
    if (!jobDomains || jobDomains.length === 0) return true;
    return userDomains.some(userDomain => jobDomains.includes(userDomain));
}

function matchesLocation(userLocations, jobLocations) {
    if (!userLocations || userLocations.length === 0) return true;
    if (!jobLocations || jobLocations.length === 0) return true;
    if (userLocations.includes('remote') || jobLocations.includes('remote')) {
        return true;
    }
    return userLocations.some(userLoc => jobLocations.includes(userLoc));
}

function matchesLeadership(userPrefs, normalizedJob) {
    const interest = userPrefs.managementInterest;
    if (!interest) {
        return !normalizedJob.leadershipLevel;
    }
    if (interest === 'no_management') {
        return !normalizedJob.leadershipLevel;
    }
    if (interest === 'management_only') {
        if (!normalizedJob.leadershipLevel) return false;
        return userPrefs.managementLevel.includes(normalizedJob.leadershipLevel);
    }
    if (interest === 'management_and_individual') {
        if (!normalizedJob.leadershipLevel) return true;
        return userPrefs.managementLevel.includes(normalizedJob.leadershipLevel);
    }
    return true;
}

/**
 * The main function that checks if a user and a job are a match across all criteria.
 * @param {object} userPreferences - The user's full preference object from Firestore.
 * @param {object} jobFromBigQuery - A raw row from the BigQuery table.
 * @returns {boolean} True if it's a match, false otherwise.
 */
export function isMatch(userPreferences, jobFromBigQuery) {
    const job = normalizeJob(jobFromBigQuery);
    const user = userPreferences;

    // --- NEW: Detailed logging as you requested ---
    console.log(`\n--- Checking Match ---`);
    console.log(`User: ${user.fullName}`);
    console.log(`Job: ${job.title}`);
    console.log(`User Prefs -> Domains: [${user.domains.join(', ')}], Locations: [${user.locations.join(', ')}]`);
    console.log(`Normalized Job -> Domains: [${job.domains.join(', ')}], Locations: [${job.locations.join(', ')}]`);

    const domainMatch = matchesDomains(user.domains, job.domains);
    console.log(`- Domain Match: ${domainMatch}`);
    if (!domainMatch) return false;
    
    const expMatch = matchesExperience(user, job);
    console.log(`- Experience Match: ${expMatch}`);
    if (!expMatch) return false;

    const locMatch = matchesLocation(user.locations, job.locations);
    console.log(`- Location Match: ${locMatch}`);
    if (!locMatch) return false;
    
    const leadershipMatch = matchesLeadership(user, job);
    console.log(`- Leadership Match: ${leadershipMatch}`);
    if (!leadershipMatch) return false;

    console.log('✅ SUCCESS: All criteria matched!');
    return true;
}