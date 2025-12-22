# nature_probleme.py

NATURE_PROBLEME = {
    "harcelement": {
        "label": "Harcèlement / climat relationnel",
        "sous_labels": {
            "harcelement_general": "Harcèlement (non précisé)",
            "harcelement_raciste": "Harcèlement raciste",
            "harcelement_islamophobe": "Harcèlement islamophobe",
            "harcelement_religieux_autre": "Harcèlement religieux (autre)",
            "harcelement_homophobe": "Harcèlement homophobe",
            "harcelement_sexiste": "Harcèlement sexiste",
            "harcelement_cyber": "Cyberharcèlement",
            "conflit_famille_etablissement": "Conflit famille / établissement",
            "conflit_inter_eleves": "Conflit entre élèves",
            "conflit_enseignant": "Conflit avec un enseignant",
            "conflit_direction": "Conflit avec la direction",
            "autre": "Autre harcèlement / conflit",
        },
    },

    "violence": {
        "label": "Violence",
        "sous_labels": {
            "violence_physique": "Violence physique",
            "violence_verbale": "Violence verbale / insultes",
            "autre": "Autre violence",
        },
    },

    "handicap_inclusion": {
        "label": "Handicap / inclusion",
        "sous_labels": {
            "handicap_non_prise_en_compte": "Handicap non pris en compte",
            "absence_aesh": "Absence d'AESH",
            "aesh_insuffisant": "Volume AESH insuffisant",
            "amenagement_non_respecte": "Aménagement non respecté",
            "orientation_uliss_ime": "Orientation ULIS / IME",
            "autre": "Autre problématique handicap / inclusion",
        },
    },

    "sante": {
        "label": "Santé",
        "sous_labels": {
            "maladie_grave": "Maladie grave impactant la scolarité",
            "sante_psychologique": "Problèmes psychologiques",
            "autre": "Autre problématique santé",
        },
    },

    "examens": {
        "label": "Examens, notes, évaluations",
        "sous_labels": {
            "contestation_note": "Contestation de note",
            "contestation_resultat": "Contestation de résultat",
            "proche_reussite": "Échec à quelques dixièmes",
            "consultation_copies": "Demande de consultation des copies",
            "erreur_materielle": "Erreur matérielle",
            "bug_numerique": "Bug informatique examen",
            "sanction_fraude": "Sanction / accusation fraude",
            "absence_justifiee_non_prise": "Absence justifiée non prise en compte",
            "absence_non_justifiee": "Absence non justifiée",
            "tiers_temps_refuse": "Tiers temps non accordé / hors délai",
            "demande_rattrapage": "Demande rattrapage / repasser",
            "jury_souverain": "Souveraineté du jury",
            "autre": "Autre problématique examen",
        },
    },

    "inscriptions_orientation": {
        "label": "Inscriptions / orientation",
        "sous_labels": {
            "pb_inscription_scolaire": "Problème inscription scolaire",
            "pb_inscription_bts": "Problème inscription BTS",
            "pb_inscription_master": "Problème inscription master",
            "pb_inscription_licence": "Problème inscription licence",
            "pb_inscription_ifsi_ifmk": "Problème inscription IFSI / IFMK / CNAM",
            "inscription_hors_delai": "Inscription hors délai",
            "refus_parcoursup": "Refus Parcoursup",
            "refus_master": "Refus admission master",
            "passage_etudes": "Passage L1/L2/L3 / redoublement",
            "reorientation": "Réorientation",
            "stage_probleme": "Problème de stage",
            "vae_refusee": "Refus / difficulté VAE",
            "autre": "Autre problématique inscription / orientation",
        },
    },

    "bourses_aides": {
        "label": "Bourses / aides financières",
        "sous_labels": {
            "refus_bourse": "Refus de bourse",
            "revision_bourse": "Révision / réexamen bourse",
            "montant_bourse": "Montant de bourse contesté",
            "droits_epuises": "Droits à bourse épuisés",
            "remboursement_bourse": "Remboursement bourse",
            "non_assiduite": "Remboursement pour non-assiduité",
            "dse_incomplet": "DSE incomplet",
            "dse_bug": "Bug sur DSE",
            "bourse_merite": "Bourse au mérite",
            "aide_financiere": "Demande aide financière",
            "autre": "Autre problématique bourse / aide",
        },
    },

    "logement": {
        "label": "Logement",
        "sous_labels": {
            "refus_logement": "Refus logement CROUS",
            "dette_logement": "Dette logement CROUS",
            "caution_non_restituee": "Caution non restituée",
            "pb_apl": "Problème APL / CAF / CROUS",
            "demande_logement": "Demande aide logement",
            "autre": "Autre problématique logement",
        },
    },

    "vie_scolaire": {
        "label": "Vie scolaire / organisation",
        "sous_labels": {
            "pb_accueil_greve": "Problème accueil (grève)",
            "pb_protocole_sanitaire": "Protocole sanitaire contesté",
            "pb_tenue": "Litige tenue",
            "accident_scolaire": "Accident scolaire",
            "objet_connecte": "Objet connecté en classe",
            "pb_absences": "Gestion des absences",
            "autre": "Autre problématique vie scolaire",
        },
    },

    "international": {
        "label": "Visa / international",
        "sous_labels": {
            "visa_refuse": "Visa d'études refusé",
            "visa_retarde": "Visa d'études retardé",
            "inscription_impossible_visa": "Inscription impossible (visa)",
            "pb_campusfrance": "Problème Campus France / consulat",
            "autre": "Autre problématique internationale",
        },
    },

    "ecoles_privees": {
        "label": "Écoles privées / organismes",
        "sous_labels": {
            "litige_frais": "Litige frais scolarité",
            "rupture_scolarite": "Rupture ou refus poursuite études",
            "absence_explications": "Absence explication résultats",
            "hors_champ_en": "Hors champ Éducation nationale",
            "autre": "Autre problématique école privée",
        },
    },

    "rh_personnels": {
        "label": "RH / carrière personnels",
        "sous_labels": {
            "pb_mutation": "Problème mutation / affectation",
            "pb_cpf": "Problème CPF / formation",
            "pb_clm_cld": "Congé maladie long (CLM/CLD)",
            "pb_retraite": "Problème retraite / IDV",
            "pb_remuneration": "Problème rémunération",
            "pb_frais": "Frais déplacement non remboursés",
            "pb_recrutement": "Recrutement / contrat",
            "autre": "Autre problématique RH",
        },
    },

    "relation_administration": {
        "label": "Relation administration",
        "sous_labels": {
            "absence_reponse": "Absence de réponse",
            "dossier_incomplet": "Dossier incomplet",
            "demande_information": "Demande d'information",
            "hors_competence": "Hors compétence du médiateur",
            "application_reglement": "Application stricte règlement",
            "inexecution_jugement": "Inexécution d'un jugement",
            "autre": "Autre problématique administrative",
        },
    },

    "autre": {
        "label": "Autre",
        "sous_labels": {
            "autre": "Autre",
        },
    },
}
